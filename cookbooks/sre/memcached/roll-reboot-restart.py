"""Memcached rolling reboot and restart cookbook."""
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta, timezone

from cumin import NodeSet
from spicerack.remote import RemoteHosts
from wmflib.constants import CORE_DATACENTERS
from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class MemcachedRollingOperation(SREBatchBase):
    """Perform rolling reboot or rolling daemon restart on memecached hosts.

    - Reboot hosts one by one or more with configurable sleep time between batches
    - Restart the memcached daemon via systemctl
    - Filter hosts by minimum uptime (e.g., only restart hosts running > 7 days)
    - Defaults: batch_default=1 and  grace_sleep=900

    Usage examples:
        # Reboot one host at a time with 15 minute intervals
        cookbook sre.memcached.roll-reboot-restart --reason "Debian reboots" \
            --alias memcached-eqiad --batchsize 1 --grace-sleep 900 reboot

        # Restart memecached daemon on specific range
        cookbook sre.memcached.roll-reboot-restart --reason "Puppet change" \
            --query 'P{mc[2040-2045].codfw.wmnet}' --batchsize 2 restart_daemons

        # Restart only hosts running for at least 10 days
        cookbook sre.memcached.roll-reboot-restart --reason "Resume reloads" \
            --alias memcached-eqiad --min-uptime 10d --batchsize 2 restart_daemons
    """

    batch_default = 1
    batch_max = 4
    grace_sleep = 900
    valid_actions = ('reboot', 'restart_daemons')

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            '--min-uptime',
            help='Minimum uptime required to restart (e.g., "7d" for days, "24h" for hours, "604800" for seconds)',
        )
        return parser

    def get_runner(self, args: Namespace) -> "MemcachedRollingOperationRunner":
        """As specified by Spicerack API."""
        runner = MemcachedRollingOperationRunner(args, self.spicerack)

        # Validate that if using an alias (not a query), either a datacenter must be specified
        # or it must be the canary alias
        if args.alias and not args.query:
            is_canary = 'canary' in args.alias
            has_datacenter = any(dc in args.alias for dc in CORE_DATACENTERS)
            if not (has_datacenter or is_canary):
                raise ValueError(
                    f"Alias '{args.alias}' must include a datacenter"
                    f"or be a canary alias. "
                    f"Specify a datacenter-specific alias like 'memcached-eqiad' or 'memcached-codfw', "
                    f"or use 'memcached-canary'."
                )
        return runner


class MemcachedRollingOperationRunner(SREBatchRunnerBase):
    """Memcached rolling reboot/restart cookbook runner."""

    # TODO: update alias list and code after T419831
    @property
    def allowed_aliases(self) -> list:
        """Return allowed aliases for memcached."""
        return [
            'memcached-eqiad',
            'memcached-codfw',
            'memcached-canary',
            'memcached-gutter-eqiad',
            'memcached-gutter-codfw',
        ]

    @property
    def restart_daemons(self) -> list:
        """Return list of daemons to restart."""
        return ['memcached', 'prometheus-memcached-exporter']

    def _parse_uptime_threshold(self, uptime_str: str) -> timedelta:
        # Parse the uptime threshold using timedelta
        if uptime_str.endswith('d'):
            days = int(uptime_str[:-1])
            return timedelta(days=days)
        if uptime_str.endswith('h'):
            hours = int(uptime_str[:-1])
            return timedelta(hours=hours)
        return timedelta(seconds=int(uptime_str))

    def _check_host_uptime(self, host: str, output, min_uptime_delta: timedelta, current_dt: datetime) -> bool:
        # Check if a host's memcached service uptime meets the threshold
        try:
            # Output format: "Thu 2025-06-26 11:21:39 UTC"
            for line in output.lines():
                timestamp_str = line.decode().strip()
                # Remove " UTC"
                timestamp_str = timestamp_str.rsplit(' ', 1)[0]
                service_start_dt = datetime.strptime(timestamp_str, '%a %Y-%m-%d %H:%M:%S').replace(
                    tzinfo=timezone.utc
                )
                service_uptime = current_dt - service_start_dt

                if service_uptime >= min_uptime_delta:
                    self.logger.info(
                        "%s: memcached uptime %gs >= threshold %gs",
                        host,
                        service_uptime.total_seconds(),
                        min_uptime_delta.total_seconds(),
                    )
                    return True
                self.logger.info(
                    "%s: memcached uptime %gs < threshold %gs (skipping)",
                    host,
                    service_uptime.total_seconds(),
                    min_uptime_delta.total_seconds(),
                )
                return False
        except (ValueError, IndexError) as exc:
            self.logger.error(
                "Failed to parse memcached service uptime on %s: %s", host, exc
            )
        return False

    def _hosts(self):
        """Get list of RemoteHosts, filtering by min-uptime if specified."""
        hosts_list = super()._hosts()

        # If min-uptime is specified, filter hosts based on memcached service uptime
        if self._args.min_uptime:
            min_uptime_delta = self._parse_uptime_threshold(self._args.min_uptime)
            filtered_nodeset = NodeSet()
            current_dt = datetime.now(timezone.utc)

            for host_group in hosts_list:
                self.logger.info(
                    "Filtering hosts by memcached service uptime: %s", self._args.min_uptime
                )

                # Get memcached service uptime using systemctl
                result = host_group.run_sync(
                    'systemctl show memcached --property=ActiveEnterTimestamp --value',
                    is_safe=True,
                )

                # Collect hosts that meet the uptime threshold
                for host, output in result:
                    if self._check_host_uptime(host, output, min_uptime_delta, current_dt):
                        filtered_nodeset.update(host)

            if not filtered_nodeset:
                raise ValueError(
                    f"No hosts available for restart. All memcached services were restarted "
                    f"more recently than the minimum uptime "
                    f"threshold ({self._args.min_uptime})."
                )

            filtered_hosts = []
            filtered_hosts.append(self._spicerack.remote().query(",".join(filtered_nodeset)))
            return filtered_hosts

        return hosts_list

    def pre_action(self, hosts: RemoteHosts) -> None:
        """As specified by Spicerack API."""
        # If we are operating on main memcached, verify the gutter pool is up
        # before every batch

        self.logger.info("Pre-action: Operating on hosts: %s", hosts.hosts)
        super().pre_action(hosts)
        alias = self._args.alias
        if alias and 'gutter' not in alias:
            # Determine which gutter pool to check
            if 'canary' in alias:
                gutter_alias = 'memcached-gutter-eqiad'
            else:
                # Find the datacenter from the alias
                for dc in CORE_DATACENTERS:
                    if dc in alias:
                        gutter_alias = f'memcached-gutter-{dc}'
                        break
            self.logger.info("Checking gutter pool %s before operating on main pool %s", gutter_alias, alias)
            gutter_hosts = self._spicerack.remote().query(f"A:{gutter_alias}")
            if not self.check_memcached_active(gutter_hosts):
                raise RuntimeError(
                    f"Gutter pool {gutter_alias} is not healthy. "
                    f"Aborting to ensure failover capacity."
                )
            self.logger.info("Gutter pool %s is healthy, proceeding with operation", gutter_alias)

    def post_action(self, hosts: RemoteHosts) -> None:
        """After action, verify memcached is active"""
        self.logger.info("Post-action: Verifying hosts: %s", hosts.hosts)
        super().post_action(hosts)

        # Note: spicerack handles timeouts at the Cumin level

        # Check if memcached is still active after the operation
        if not self.check_memcached_active(hosts):
            self.logger.warning(
                "Memcached is not active on all hosts after %s", self._args.action
            )

    def check_memcached_active(self, hosts: RemoteHosts) -> bool:
        """Check if memcached service is active on all given hosts."""
        target_host = ",".join(hosts.hosts)

        if self._spicerack.dry_run:
            self.logger.info("DRY-RUN: Would check memcached active on hosts: %s", target_host)
            return True

        try:
            result = hosts.run_sync(
                'systemctl is-active memcached',
                is_safe=True,
            )

            # Check that all hosts returned 'active'
            failed_hosts = []
            for host, output in result:
                for line in output.lines():
                    status = line.decode().strip()
                    if status != 'active':
                        failed_hosts.append(f"{host} (status: {status})")

            if failed_hosts:
                self.logger.error(
                    "Memcached not active on %d host(s): %s",
                    len(failed_hosts),
                    ", ".join(failed_hosts),
                )
                return False

            self.logger.info("All hosts in %s have memcached active", target_host)
            return True
        except (OSError, RuntimeError) as exc:
            self.logger.error("Memcached check failed for hosts: %s", exc)
            return False
