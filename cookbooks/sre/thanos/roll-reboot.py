"""Roll-reboot a Thanos cluster one host at a time"""

# Per host, one at a time:
#   1. Depool the host via conftool, if it has an entry.
#   2. Reboot (inherently sets downtime).
#   3. Wait for the host back online, then wait for its first puppet run.
#   4. Confirm Thanos services, memcached, and all units are healthy.
#   5. If step 4 passes repool the host, otherwise leave it depooled and raise.
#   6. Wait grace_sleep, then proceed to the next host.
#
# Usage:
#     cookbook sre.thanos.roll-reboot \
#         --alias titan-eqiad \
#         --reason "Rolling reboot for kernel update" \
#         reboot

import logging
import re
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import Optional

from spicerack import Spicerack
from spicerack.confctl import ConfctlError
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)

CLUSTERGROUPS = ("titan-eqiad", "titan-codfw")

KNOWN_UNIT_PREFIXES = (
    "thanos-sidecar.service",
    "thanos-store@",
    "thanos-compact.service",
    "thanos-receive.service",
    "thanos-query.service",
    "thanos-query-frontend.service",
    "thanos-rule@",
    "thanos-bucket-web.service",
)


def _is_known_unit(unit: str) -> bool:
    """True if `unit` belongs to one of the families above."""
    return any(
        unit == family or (family.endswith("@") and unit.startswith(family))
        for family in KNOWN_UNIT_PREFIXES
    )


KNOWN_PORT_OVERRIDES: dict = {}

# Regex to pull a port number out of a --http-address flag on a unit's ExecStart line.
HTTP_ADDRESS_RE = re.compile(r"--http-address[= ]\S*:(\d+)")

# Oneshot config-reload helpers and thanos.slice are excluded from the health-check set.
EXCLUDED_UNITS = {
    "thanos-rule-reload@main.service",
    "thanos-rule-reload@pilot.service",
    "thanos.slice",
}

# Units whose /-/healthy and /-/ready check is mandatory regardless of --skip-http-checks
MANDATORY_HTTP_CHECK_UNITS = {
    "thanos-query.service",
    "thanos-query-frontend.service",
    "thanos-sidecar.service",
    "thanos-receive.service",
}


def _is_mandatory_check(unit: str) -> bool:
    """True if `unit` must always be checked via the HTTP API."""
    return unit.startswith("thanos-store@") or unit in MANDATORY_HTTP_CHECK_UNITS


def _needs_long_retry_budget(unit: str) -> bool:
    """True if `unit` has a legitimately slow startup needing the
    long retry budget."""
    # thanos-store and thanos-receive both have legitimately slow startups (index-header loading / WAL replay).
    return unit.startswith("thanos-store@") or unit == "thanos-receive.service"


# These hosts also run memcached, confirmed healthy before repooling (see check_memcached()).
MEMCACHED_UNIT = "memcached.service"
MEMCACHED_PORT = 11211


class ThanosComponentNotReady(Exception):
    """Raised when a Thanos unit or memcached isn't healthy yet."""


class RollingOperation(SREBatchBase):
    """Roll-reboot a Thanos cluster, one host at a time (reboot only)."""

    grace_sleep = 120
    min_grace_sleep = 60
    # --batchsize should never raise this above 1.
    batch_max = 1

    def argument_parser(self) -> ArgumentParser:
        """Add cookbook-specific arguments and restrict the action to
        reboot only."""

        parser = super().argument_parser()
        for action in parser._actions:
            if action.dest == "action":
                action.choices = ["reboot"]
                action.help = "This cookbook only supports 'reboot'."
        parser.add_argument(
            "--skip-http-checks",
            action="store_true",
            help="Skip HTTP /-/healthy and /-/ready polling for "
                 "non-mandatory units and rely on systemd unit "
                 "active-state only. Has no effect on thanos-store, "
                 "thanos-receive, thanos-query, thanos-query-frontend, "
                 "or thanos-sidecar, which are always checked via the "
                 "API.",
        )
        return parser

    def get_runner(self, args: Namespace):
        """As specified by Spicerack API."""

        return RollingOperationRunner(args, self.spicerack)


class RollingOperationRunner(SREBatchRunnerBase):
    """Reboot Thanos hosts one at a time, depooling/repooling and
    confirming health around each reboot."""

    depool_sleep = 20

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Required by SREBatchRunnerBase"""

        self._confctl = spicerack.confctl("node")
        self._skip_http_checks = args.skip_http_checks
        self._pooled_entries_this_batch: list = []
        self._discovered_ports: dict = {}

        super().__init__(args, spicerack)

    @property
    def allowed_aliases(self) -> list:
        """Required by SREBatchRunnerBase"""
        return list(CLUSTERGROUPS)

    @staticmethod
    def _host_regex(hosts) -> str:
        """Anchored, escaped alternation pattern for confctl's name=
        tag matching exactly these hosts."""

        return "(?:" + "|".join(re.escape(h) for h in hosts) + ")"

    def _pooled_entries(self, hosts: RemoteHosts) -> list:
        """Return the conftool node entry objects for hosts in this
        batch that have one, i.e. need depool/repool around the reboot."""

        try:
            return list(self._confctl.get(name=self._host_regex(hosts.hosts)))
        except ConfctlError:
            logger.info("No hosts in this batch have a conftool entry, "
                        "skipping depool/pool")
            return []

    def _set_pooled(self, entries: list, value: str) -> None:
        """Set conftool's 'pooled' value for these already-fetched entries."""

        if not entries:
            return
        self._confctl.update_objects({"pooled": value}, entries)
        logger.info("Set pooled=%s for: %s", value,
                    sorted(str(entry.name) for entry in entries))

    def active_thanos_units(self, hosts: RemoteHosts) -> dict:
        """Discover which Thanos units are actually active on each host."""

        result: dict = {}
        query = hosts.run_sync(
            "systemctl list-units --type=service --state=active "
            "--no-legend --plain 'thanos-*.service'",
            is_safe=True,
            print_progress_bars=False,
            print_output=False,
        )
        for nodeset, output in query:
            active = {
                line.split()[0]
                for line in output.message().decode().splitlines()
                if line.strip()
            }
            matched = sorted(
                unit for unit in active
                if _is_known_unit(unit) and unit not in EXCLUDED_UNITS
            )
            for host in nodeset:
                result[str(host)] = matched
        return result

    def discover_port(self, host: str, unit: str) -> Optional[int]:
        """Read `unit`'s systemd ExecStart line on `host` and return
        its --http-address port, or None if not found."""

        remote_host = self._spicerack.remote().query(f"D{{{host}}}")
        result = remote_host.run_sync(
            f"systemctl show {unit} --property=ExecStart --value",
            is_safe=True,
            print_progress_bars=False,
            print_output=False,
        )
        for _, output in result:
            match = HTTP_ADDRESS_RE.search(output.message().decode())
            if match:
                return int(match.group(1))
        return None

    def _get_port(self, host: str, unit: str) -> Optional[int]:
        """Cached wrapper around discover_port()/KNOWN_PORT_OVERRIDES."""

        if unit in KNOWN_PORT_OVERRIDES:
            return KNOWN_PORT_OVERRIDES[unit]
        key = (host, unit)
        if key not in self._discovered_ports:
            self._discovered_ports[key] = self.discover_port(host, unit)
        return self._discovered_ports[key]

    def _probe_status(self, host: str, unit: str, port: int, path: str) -> bool:
        """Single HTTP probe; returns True/False rather than raising."""

        url = f"http://{host}:{port}{path}"
        logger.info("Health checking %s via %s", unit, url)
        remote_host = self._spicerack.remote().query(f"D{{{host}}}")
        try:
            result = remote_host.run_sync(
                f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 5 {url}",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
        except RemoteExecutionError:
            return False
        for _, output in result:
            return output.message().decode().strip() == "200"
        return False

    def _checks_once(self, host: str, unit: str, port: int) -> None:
        """Probe both /-/healthy and /-/ready and log."""

        healthy_ok = self._probe_status(host, unit, port, "/-/healthy")
        ready_ok = self._probe_status(host, unit, port, "/-/ready")
        both_ok = healthy_ok and ready_ok

        logger.info(
            "%s %s %s healthy and %s ready, %s",
            host, unit,
            "is" if healthy_ok else "is not",
            "is" if ready_ok else "is not",
            "proceeding" if both_ok else "retrying",
        )

        if not both_ok:
            raise ThanosComponentNotReady(
                f"[{host}] {unit} not OK "
                f"(healthy={healthy_ok}, ready={ready_ok})")

    @retry(
        tries=30,
        delay=timedelta(seconds=10),
        backoff_mode="constant",
        exceptions=(ThanosComponentNotReady),
    )
    def _wait_ready_default(self, host: str, unit: str, port: int) -> None:
        """~5 minute retry budget for components that come up quickly
        or are genuinely stuck."""
        self._checks_once(host, unit, port)

    @retry(
        tries=120,
        delay=timedelta(seconds=60),
        backoff_mode="constant",
        exceptions=(ThanosComponentNotReady),
    )
    def _wait_ready_long_budget(self, host: str, unit: str, port: int) -> None:
        """~120 minute retry budget for units in
        _needs_long_retry_budget(), which can legitimately take a
        long time to start."""
        self._checks_once(host, unit, port)

    def check_component_ready(self, host: str, unit: str) -> None:
        """Confirm a single unit is healthy before we'll repool the
        host it's on."""

        port = self._get_port(host, unit)
        mandatory = _is_mandatory_check(unit)

        if mandatory:
            if port is None:
                raise RuntimeError(
                    f"Could not determine an HTTP port for {unit} on "
                    f"{host} by reading its systemd ExecStart line. "
                )
            if self._skip_http_checks:
                logger.warning(
                    "--skip-http-checks was set, but checking %s on "
                    "%s via the API anyway -- systemd 'active' alone "
                    "isn't a reliable health signal for %s.",
                    unit, host, unit)
            if _needs_long_retry_budget(unit):
                self._wait_ready_long_budget(host, unit, port)
            else:
                self._wait_ready_default(host, unit, port)
            return

        if self._skip_http_checks or port is None:
            logger.info(
                "No HTTP readiness check available for %s on %s "
                "(port not discoverable or --skip-http-checks set); "
                "relying on systemd active-state only", unit, host)
            return
        self._wait_ready_default(host, unit, port)

    @retry(
        tries=12,
        delay=timedelta(seconds=10),
        backoff_mode="constant",
        exceptions=(ThanosComponentNotReady,),
    )
    def check_memcached(self, host: str) -> None:
        """Confirm memcached is both active and responding to its own protocol after reboot."""

        remote_host = self._spicerack.remote().query(f"D{{{host}}}")

        active_result = remote_host.run_sync(
            f"systemctl is-active {MEMCACHED_UNIT}", is_safe=True,
            print_progress_bars=False, print_output=False)
        state = None
        for _, output in active_result:
            state = output.message().decode().strip()
        is_active = state == "active"

        responsive = False
        if is_active:
            probe_result = remote_host.run_sync(
                f"printf 'version\\r\\n' | nc -w2 127.0.0.1 "
                f"{MEMCACHED_PORT}",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
            for _, output in probe_result:
                responsive = output.message().decode().startswith(
                    "VERSION")

        both_ok = is_active and responsive
        logger.info(
            "%s %s %s active and %s responsive to the memcached "
            "protocol, %s",
            host, MEMCACHED_UNIT,
            "is" if is_active else "is not",
            "is" if responsive else "is not",
            "proceeding" if both_ok else "retrying",
        )

        if not both_ok:
            raise ThanosComponentNotReady(
                f"[{host}] memcached not OK "
                f"(active={is_active}, responsive={responsive})")

    @retry(
        tries=6,
        delay=timedelta(seconds=10),
        backoff_mode="constant",
        exceptions=(ThanosComponentNotReady,),
    )
    def check_no_failed_units(self, host: str) -> None:
        """Confirm nothing else on the host failed to (re)start after the reboot."""

        remote_host = self._spicerack.remote().query(f"D{{{host}}}")
        result = remote_host.run_sync(
            "systemctl --failed --no-legend --plain", is_safe=True,
            print_progress_bars=False, print_output=False)
        failed = []
        for _, output in result:
            failed = [
                line.split()[0]
                for line in output.message().decode().splitlines()
                if line.strip()
            ]
        ok = not failed

        logger.info(
            "%s %s failed systemd unit(s)%s, %s",
            host,
            "has no" if ok else f"has {len(failed)}",
            "" if ok else f" ({', '.join(failed)})",
            "proceeding" if ok else "retrying",
        )

        if not ok:
            raise ThanosComponentNotReady(
                f"[{host}] failed systemd units present: {failed}")

    def check_host_healthy(self, host: str, units: list) -> None:
        """Run every health check for a single host."""

        if not units:
            logger.warning(
                "No known thanos-*.service units active on %s", host)
        for unit in units:
            self.check_component_ready(host, unit)
        self.check_memcached(host)
        self.check_no_failed_units(host)

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Depool this host, if it has a conftool entry, before the base class reboots it."""

        entries = self._pooled_entries(hosts)
        self._pooled_entries_this_batch = entries
        if not entries:
            logger.info("No conftool entry for %s, skipping depool",
                        list(hosts.hosts))
            return
        self._set_pooled(entries, "no")
        self._sleep(self.depool_sleep)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Confirm healthy then repool"""

        active = self.active_thanos_units(hosts)
        for host, units in active.items():
            self.check_host_healthy(host, units)

        if self._pooled_entries_this_batch:
            self._set_pooled(self._pooled_entries_this_batch, "yes")
        else:
            logger.info("Nothing was depooled for %s, skipping repool",
                        list(hosts.hosts))
