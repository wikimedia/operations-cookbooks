"""Gerrit topology validation cookbook.

This cookbook manages Gerrit sanity checks.
it checks:
    - users running Gerrit
    - liveliness of the systemd unit
    - DNS
    - replica status of the hosts
It is first intended as a helper cookbook for sre.gerrit.failover
It can also be used as a standalone cookbook to make sure the configuration
is at an expected state before/after any Gerrit operation
"""

import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta

from spicerack.decorators import retry
from spicerack.cookbook import CookbookInitSuccess
from wmflib.interactive import ensure_shell_is_durable
from wmflib.dns import Dns
from cookbooks.sre import CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)


class GerritTopologyChecker(CookbookBase):
    """CLI wrapper for the Gerrit topology checker."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()

        # full check
        parser.add_argument("--source", help="Current primary Gerrit host")
        parser.add_argument("--replica", help="Current replica host")
        parser.add_argument(
            "--chown",
            action="store_true",
            default=False,
            help=(
                "Ignore daemon-user mismatch between hosts. The calling cookbook "
                "is responsible for any ownership fixes."
            ),
        )
        parser.add_argument(
            "--full",
            action="store_true",
            default=False,
            help=(
                "Run all validation steps and report every issue at the end "
                "instead of aborting on first failure."
            ),
        )

        # systemd only mode
        parser.add_argument(
            "--systemd",
            action="store_true",
            default=False,
            help=(
                "Skip everything and just check whether the replica flag "
                "is present in the systemd units. Must be used with --host."
            ),
        )
        parser.add_argument(
            "--host",
            help="Gerrit host to check for '--replica', mandatory with --systemd.",
        )

        parser.add_argument(
            "--dns",
            action="store_true",
            default=False,
            help=(
                "Only verify that --source and --replica resolve "
                "gerrit.wikimedia.org to the same A/AAAA record and exit. "
                "Must be used together with --source and --replica."
            ),
        )

        return parser

    def get_runner(self, args):
        """Instantiate the runner used by Spicerack."""
        return TopologyCheckerRunner(args, self.spicerack)


class TopologyCheckerRunner(CookbookRunnerBase):
    """Runner that executes the topology validations."""

    max_concurrency = 1
    lock_ttl = 900

    def __init__(self, args, spicerack):
        """Collect all data required for the selected check type."""
        ensure_shell_is_durable()
        self.args = self.parse_and_validate_args(args)
        self.spicerack = spicerack
        self.errors = []

        if self.args.dns:
            self.dns = Dns()
            self.src = spicerack.remote().query(f"{args.source}.*")
            self.replica = spicerack.remote().query(f"{args.replica}.*")
            self.service_ip = self.dns.resolve_ipv4("gerrit.wikimedia.org")[0]
            logger.info("Authoritative A record for gerrit.wikimedia.org: %s", self.service_ip)

            self._assert_dns_consistent()
            raise CookbookInitSuccess(
                "DNS validation succeeded: both hosts resolve gerrit.wikimedia.org to "
                f"{self.service_ip}."
            )

        if self.args.systemd:
            self.host = spicerack.remote().query(f"{args.host}.*")
            if not self._has_replica_flag(self.host):
                raise RuntimeError(f"{self.host.hosts[0]} has NO replica flag.")
            raise CookbookInitSuccess(f"{self.host.hosts[0]} has a replica flag.")

        self.dns = Dns()
        self.src = spicerack.remote().query(f"{args.source}.*")
        self.replica = spicerack.remote().query(f"{args.replica}.*")
        puppet = spicerack.puppet_server()
        self.src_user = (
            puppet.hiera_lookup(self.src.hosts[0], "profile::gerrit::daemon_user").splitlines()[-1]
        )
        self.replica_user = (
            puppet.hiera_lookup(self.replica.hosts[0], "profile::gerrit::daemon_user").splitlines()[-1]
        )
        self.service_ip = self.dns.resolve_ipv4("gerrit.wikimedia.org")[0]
        logger.info("Authoritative A record for gerrit.wikimedia.org: %s", self.service_ip)

    def parse_and_validate_args(self, args: Namespace) -> Namespace:
        """Validate CLI flags combinations.

        Quick help:
            * --systemd   → requires --host, incompatible with --source/--replica/--chown/--dns
            * --dns       → requires --source & --replica, incompatible with --systemd/--chown/--full/--host
            * default     → requires --source & --replica
        """
        # Incompatible flag combos -------------------------------------------------
        if args.systemd and args.dns:
            raise RuntimeError("--systemd is incompatible with --dns")

        if args.systemd:
            if args.source is not None:
                raise RuntimeError("--systemd is incompatible with --source")
            if args.replica is not None:
                raise RuntimeError("--systemd is incompatible with --replica")
            if args.chown:
                raise RuntimeError("--systemd is incompatible with --chown")
            if args.full:
                raise RuntimeError("--full is meaningless with --systemd; omit it")
            if args.dns:
                raise RuntimeError("--dns is incompatible with --systemd")
            if args.host is None:
                raise RuntimeError("--host is required when --systemd is used")
            return args

        if args.dns:
            if args.source is None:
                raise RuntimeError("--source is required when --dns is used")
            if args.replica is None:
                raise RuntimeError("--replica is required when --dns is used")
            return args

        if args.source is None:
            raise RuntimeError("--source is required unless --systemd or --dns is used")
        if args.replica is None:
            raise RuntimeError("--replica is required unless --systemd or --dns is used")
        if args.host is not None:
            raise RuntimeError("--host is only valid together with --systemd")
        return args

    @property
    def runtime_description(self) -> str:
        """Return a one-line description used by Spicerack."""
        if self.args.dns:
            return (
                "Validate Gerrit DNS consistency (source="
                f"{self.args.source}, replica={self.args.replica})"
            )
        if self.args.systemd:
            return f"Validate Gerrit replica flag on {self.args.host}"
        return (
            "Validate Gerrit topology (source="
            f"{self.args.source}, replica={self.args.replica})"
        )

    def run(self):
        """Entry-point when executed standalone from the CLI."""
        # DNS-only and systemd-only paths exit early via CookbookInitSuccess
        if self.args.dns or self.args.systemd:
            return
        self._validate_topology()
        logger.info("Topology validation succeeded.")

    def validate(self) -> None:
        """Expose the validation sequence for programmatic use."""
        if self.args.dns or self.args.systemd:
            return
        self._validate_topology()

    def _validate_topology(self) -> None:
        """Run the ordered set of topology checks (full mode)."""
        logger.info("Running Gerrit topology checks")

        # 1. gerrit.service is active on both hosts
        for host in (self.src, self.replica):
            if not self._is_unit_active(host):
                self._record_error(f"gerrit.service is not active on {host.hosts[0]}")

        # 2. Replica flag appears only on replica
        if not self._has_replica_flag(self.replica):
            self._record_error("Replica host is missing the 'replica' flag in systemd unit.")
        if self._has_replica_flag(self.src):
            self._record_error("Source host unexpectedly carries a 'replica' flag.")

        # 3. DNS consistency across hosts
        try:
            self._assert_dns_consistent()
        except RuntimeError as e:
            self._record_error(str(e))

        # 4. Daemon user identical unless --chown was supplied
        if self.src_user != self.replica_user and not self.args.chown:
            self._record_error(
                "Gerrit user differs between hosts. Use --chown if that is expected."
            )

        if self.args.full and self.errors:
            raise RuntimeError(
                "Topology validation failed with the following issues:\n- " + "\n- ".join(self.errors)
            )

    def _record_error(self, message: str) -> None:
        """Handle an error according to the chosen mode."""
        if self.args.full:
            self.errors.append(message)
            logger.error(message)
        else:
            raise RuntimeError(message)

    def _is_unit_active(self, host) -> bool:
        result = list(
            host.run_sync(
                "systemctl show -p ActiveState --value gerrit.service",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
        )
        state = result[0][1].message().decode().strip()
        logger.debug("%s: gerrit.service → %s", host.hosts[0], state)
        return state == "active"

    def _has_replica_flag(self, host) -> bool:
        cmd = (
            "grep -q replica "
            "/etc/systemd/system/multi-user.target.wants/gerrit.service "
            "&& echo FOUND || echo MISSING"
        )
        result = list(
            host.run_sync(
                cmd,
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
        )
        flag = result[0][1].message().decode().strip()
        logger.debug("%s: replica flag → %s", host.hosts[0], flag)
        return flag == "FOUND"

    def _dig(self, host, fqdn: str) -> str:
        result = list(
            host.run_sync(
                f"dig +short {fqdn}",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
        )
        return result[0][1].message().decode().strip()

    @retry(
        tries=40,
        delay=timedelta(seconds=15),
        backoff_mode="constant",
        failure_message="DNS propagation timed out",
        exceptions=(RuntimeError,),
    )
    def _assert_dns_consistent(self) -> None:
        inconsistent = {}
        for host in (self.src, self.replica):
            ip = self._dig(host, "gerrit.wikimedia.org")
            if ip != self.service_ip:
                inconsistent[host.hosts[0].name] = ip
        if inconsistent:
            raise RuntimeError(f"DNS resolution mismatch: {inconsistent}")
        logger.info("Both hosts resolve gerrit.wikimedia.org → %s", self.service_ip)
