"""Pool a DB in dbctl."""

import json
import logging
import re
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, fields
from datetime import timedelta
from time import sleep
from typing import Optional, Tuple
from urllib.request import urlopen

from conftool.extensions.dbconfig.action import ActionResult
from conftool.extensions.dbconfig.entities import Instance as DBCInst
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.mysql import ensure, get_mysqlremotehosts
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.icinga import HostsStatus as IcingaHostsStatus
from spicerack.icinga import HostStatus as IcingaStatus
from spicerack.icinga import IcingaHosts, IcingaStatusNotFoundError
from spicerack.mysql import Instance as MInst
from spicerack.mysql import MysqlRemoteHosts
from spicerack.remote import RemoteHosts
from wmflib.interactive import ensure_shell_is_durable

# TODO: improve handling of spurious changes, right now it bails out

# TODO: use httpx ideally


hostname_regex = re.compile(r"[a-z][a-z-]*[a-z](\d{4})")
log = logging.getLogger(__name__)


def step(slug: str, msg: str) -> None:
    """Logging helper."""
    log.info("[%s.%s] %s", __name__, slug, msg)


@dataclass
class InstanceMetadata:
    """Instance metadata"""

    alerts: list
    dc: str
    instance_group: str
    instance_name: str
    is_candidate_on_dbctl: Optional[bool]
    is_lagging: bool
    # kernel_version: str | None
    lag: float
    mariadb_version: Optional[str]
    pooled_value: int
    preferred_candidate: bool
    role: str
    section_kind: str
    section: str
    tags: list
    uptime_human: str
    uptime_s: int


def _jget(url: str) -> dict:
    """Fetch json dict"""
    log.debug(f"Fetching {url}")
    with urlopen(url, timeout=5) as resp:  # nosec B310
        j = json.loads(resp.read())
    return j


def fetch_host_instance_from_zarcillo(hn: str) -> InstanceMetadata:
    """Fetch InstanceMetadata"""
    url = f"https://zarcillo.wikimedia.org/api/v1/instances/{hn}"
    j = _jget(url)
    if "instances" not in j:
        raise RuntimeError(f"Unexpected response {j}")

    icnt = len(j["instances"])
    if icnt > 1:
        raise RuntimeError(f"{icnt} instances found on {hn}. Multi-instance hosts are not supported.")
    elif icnt < 1:
        raise RuntimeError(f"No instances found on {hn}.")

    i = j["instances"][0]
    # logger.debug(f"Received {i}")

    field_names = {f.name for f in fields(InstanceMetadata)}
    d = {k: v for k, v in i.items() if k in field_names}

    d["section_kind"], _ = extract_section_kind_and_method(i["section"])

    return InstanceMetadata(**d)


def extract_section_kind_and_method(section_name: str) -> Tuple[str, str]:
    """Extract section king e.g. s3 -> s and pooling method"""
    kind = section_name.rstrip("0123456789")
    # unsupported:
    # "analytics_meta",
    # "backup1-codfw",
    # "backup1-eqiad",
    # "labservices",
    # "labtestservices",
    # "m",
    # "matomo",
    # "tendril",
    # "staging",
    # "test-s",
    supported = {
        "ms": "pc",  # parsercache cookbook
        "pc": "pc",  # parsercache cookbook
        "es": "s",
        "s": "s",
        "x": "s",
    }
    if kind not in supported:
        raise RuntimeError(f"Unsupported section kind: {kind}")

    return kind, supported[kind]


def get_minst(mrhs: MysqlRemoteHosts) -> MInst:
    instances: list[MInst] = mrhs.list_hosts_instances()
    return instances[0]


def validate_hostname_extract_dc_fqdn(hn_or_fqdn: str) -> tuple[str, str, str]:
    """Given a hostname or FQDN, validates it and return the hostname, DC and FQDN"""
    if "." in hn_or_fqdn:
        hn, tmp_dc, _ = hn_or_fqdn.split(".", 2)
    else:
        hn, tmp_dc = hn_or_fqdn, ""

    m = hostname_regex.fullmatch(hn)
    if m is None:
        raise ValueError(f"Invalid hostname '{hn}'")

    dcnum = m.group(1)[0]
    ensure(dcnum in ("1", "2"), f"Invalid hostname '{hn}'")
    if dcnum == "1":
        dc = "eqiad"
    else:
        dc = "codfw"

    fqdn = f"{hn}.{dc}.wmnet"

    if "." in hn_or_fqdn:
        ensure(tmp_dc == dc and fqdn == hn_or_fqdn, f"Invalid FQDN '{hn_or_fqdn}'")

    return (hn, dc, fqdn)


def _run_cmd(host: RemoteHosts, cmd: str, is_safe: bool = False) -> str:
    out = host.run_sync(cmd, is_safe=is_safe, print_progress_bars=False, print_output=False)
    # TODO: cleanup
    return list(out)[0][1].message().decode("utf-8")


def _fetch_replication_delay_ms(ins: MInst) -> int:
    sql = """
    SELECT TIMESTAMPDIFF(MICROSECOND, max(ts), UTC_TIMESTAMP(6)) AS delta_us
    FROM heartbeat.heartbeat ORDER BY ts LIMIT 1 """
    r = ins.fetch_one_row(sql)
    return int(r["delta_us"] / 1_000)


def _fetchall(ins: MInst, sql: str, args: tuple) -> tuple[dict]:
    with ins.cursor() as (_conn, cur):
        _ = cur.execute(sql, args)
        res = tuple(cur.fetchall())
        ins.check_warnings(cur)
        return res


def _poll_icinga_notification_status(icinga_host: IcingaHosts, hostname: str) -> None:
    notif_disabled_msg = f"""Host notifications are disabled on Icinga. Check puppet for
    `profile::monitoring::notifications_enabled: false`
    in hieradata/hosts/{hostname}.yaml
    """
    for attempt in range(100):
        try:
            ihs: IcingaHostsStatus = icinga_host.get_status()
            s: IcingaStatus = ihs[hostname]
            if s.notifications_enabled:
                return
            log.info(notif_disabled_msg)
        except (IcingaStatusNotFoundError, KeyError):
            log.info("The host is unknown to Icinga: you might need to add it to puppet.")

        log.debug("[%s] polling again in 5 minutes...", attempt)
        sleep(60 * 5)

    raise RuntimeError("Timed out while waiting for Icinga notifications to be enabled")


class Pool(CookbookBase):
    """Pool a DB instance in dbctl and allow to gradually increase its pooled percentage.

    There are three available profiles to control the repool steps. All of them use a power of two progression for
    increasing the percentage from 0% to 100%.

    The default profile does it in 4 steps. There are also a fast profile with just 2 steps and a slow one with 10
    steps.

    The current sleep between steps is 15 minutes.

    Examples:
        # Pool the instance gradually sleeping in between steps
        sre.mysql.pool -r "Some reason" db1001

        # Pool the instance and update a Phabricator task at the start and end of the pooling operation
        sre.mysql.pool -r "Some reason" -t T12345 db1001

        # Pool the instance quickly with just two steps
        sre.mysql.pool -r "Some reason" --fast db1001

    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("--skip-safety-checks", action="store_true", help="Skip checking for Icinga status")
        profile = parser.add_mutually_exclusive_group()
        profile.add_argument(
            "--fast",
            action="store_true",
            help="Repool the host quicker with just two steps.",
        )
        profile.add_argument(
            "--slow",
            action="store_true",
            help="Repool the host more slowly, with ten steps.",
        )

        # TODO: add support for multiple instances? Based on what? (puppetdb, dbctl, orchestrator)
        parser.add_argument("instance", help="Hostname or FQDN")

        return parser

    def get_runner(self, args: Namespace) -> "PoolRunner":
        """As specified by Spicerack API."""
        return PoolRunner(args, self.spicerack)


class PoolRunner(CookbookRunnerBase):
    """Pool a MySQL instance cookbook runner."""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """As specified by Spicerack API."""
        # Silence some more noisy loggers for the dry-run mode
        logging.getLogger("etcd.client").setLevel(logging.INFO)
        logging.getLogger("conftool").setLevel(logging.INFO)

        self.args = args

        self.dbctl = spicerack.dbctl()
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
        self.task_id = args.task_id
        self.dry_run = spicerack.dry_run
        self._mysql = spicerack.mysql()
        self._run_cookbook = spicerack.run_cookbook

        ensure_shell_is_durable()

        if self.args.slow:
            self.steps: tuple[int, ...] = (
                1,
                4,
                9,
                16,
                25,
                36,
                49,
                64,
                81,
                100,
            )  # 10 steps, power or 2 progression
        elif self.args.fast:
            self.steps = (25, 100)  # 2 steps, power of 2 progression
        else:
            self.steps = (6, 25, 56, 100)  # 4 steps, power of 2 progression

        hostname, _dc, fqdn = validate_hostname_extract_dc_fqdn(args.instance)

        self._mrhs = get_mysqlremotehosts(spicerack, fqdn)
        self._mysql_instance: MInst = get_minst(self._mrhs)

        dbi: DBCInst = self.dbctl.instance.get(hostname)
        ensure(dbi is not None, f"Unable to find instance {hostname} in dbctl. Aborting.")
        ensure(dbi.name == hostname, f"Incorrect host found {dbi.name} vs {hostname}")
        self._hostname = hostname

        self.datacenter = dbi.tags.get("datacenter")

        self._icinga_host = spicerack.icinga_hosts(self._mrhs.remote_hosts.hosts)
        # self._alerting_hosts = spicerack.alerting_hosts(self._mrhs.remote_hosts.hosts)

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return f"pool {self.args.instance}: {self.reason.reason}"

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-instance."""
        # TTL includes both the sleep time (900s) plus the potential retries for wait_diff_clean (30*30s) for each step
        ttl = 1800 * len(self.steps)
        return LockArgs(suffix=self.args.instance, concurrency=1, ttl=ttl)

    def _update_phabricator(self, status: str, desc: str) -> None:
        msg = f"{status} {desc} by {self.reason.owner}: {self.reason.reason}"
        if self.phabricator.task_accessible(self.task_id, raises=False):
            self.phabricator.task_comment(self.task_id, msg, raises=False)
        else:
            log.warning(f"Unable to access task {self.task_id}: not adding comment '{msg}'")

    def check_action_result(self, action_result: ActionResult, message: str) -> None:
        """Raise on failure and log any messages present in an ActionResult instance."""
        for result_message in action_result.messages:
            log.log(logging.INFO if action_result.success else logging.ERROR, result_message)

        if action_result.announce_message:
            log.info(action_result.announce_message)

        if not action_result.success:
            raise RuntimeError(f"Failed to {message}")

    def _pool_s_or_es(self) -> None:
        if self.args.skip_safety_checks is False:
            _poll_icinga_notification_status(self._icinga_host, self._hostname)
            log.debug("Waiting for icinga to go green")
            self._icinga_host.wait_for_optimal()

        self._update_phabricator("Starting", f"pool of {self.args.instance}")
        self.gradual_pooling()

    def _pool_pc_or_ms(self, section: str) -> None:
        if self.args.skip_safety_checks:
            raise RuntimeError("Flag not supported")

        cmar = []
        if self.args.reason:
            cmar.extend(["--reason", self.args.reason])

        if self.args.task_id:
            cmar.extend(["--task-id", self.args.task_id])

        cmar.extend([section, "pool"])
        self._run_cookbook("sre.mysql.parsercache", cmar)

    def run(self) -> None:
        """As required by the Spicerack API."""
        try:
            imeta = fetch_host_instance_from_zarcillo(self.args.instance)
            section = imeta.section
        except Exception as e:
            log.error(f"Error {e}")
            log.info("If you want to continue anyway input the section: ")
            section = input("Section: ").strip().lower()

        _, pool_method = extract_section_kind_and_method(section)

        if pool_method == "pc":
            log.info("Using parsercache cookbook")
            log.info(f"The whole '{section}' section will be pooled")
            self._pool_pc_or_ms(section)

            # currently parsercache cookbook does its own phab updating
            # self._update_phabricator("Completed", msg)

        elif pool_method == "s":
            msg = f"pooling of {self.args.instance}"
            self._pool_s_or_es()
            self._update_phabricator("Completed", msg)

    def _fetch_current_pooling(self, i: str, percentage: int) -> set[tuple[bool, bool]]:
        ensure("." not in i, f"dbctl.instance.get not supporting FQDN {i}")
        instance = self.dbctl.instance.get(i)
        ensure(instance is not None, f"dbctl instance for {i} not found")
        current_pooling = {
            (section["pooled"], section["percentage"] >= percentage) for section in instance.sections.values()
        }
        return current_pooling

    def gradual_pooling(self) -> None:
        """Gradually pool the instance with increasing percentages."""
        sleep_duration = 5 if self.dry_run else 900
        for percentage in self.steps:
            hostname, _dc, _fqdn = validate_hostname_extract_dc_fqdn(self.args.instance)
            current_pooling = self._fetch_current_pooling(hostname, percentage)
            # Skip if all the sections are pooled with a percentage equal or greater than the percentage to set
            if len(current_pooling) == 1 and current_pooling.pop() == (True, True):
                msg = "Skipping pooling instance %s at %d%%: instance already pooled with higher percentage"
                log.info(msg, self.args.instance, percentage)
                continue

            msg = f"Pooling instance {self.args.instance} at {percentage}%"
            log.info(msg)
            self.wait_diff_clean()
            ret = self.dbctl.instance.pool(self.args.instance, percentage=percentage)
            self.check_action_result(ret, msg)
            self.commit_change(msg)
            if percentage == 100:
                log.debug("pooling-in completed")
                return

            sleep(sleep_duration)

    def commit_change(self, message: str) -> None:
        """Check the diff and commit the changepy."""
        ret = self.get_diff()
        self.check_action_result(ret, f"get diff to {message}")

        ret = self.dbctl.config.commit(batch=True, datacenter=self.datacenter, comment=self.reason.reason)
        self.check_action_result(ret, f"commit change to {message}")

    @retry(
        tries=30,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        failure_message="Waiting for dbctl config diff to be clean",
        exceptions=(RuntimeError,),
    )
    def wait_diff_clean(self) -> None:
        """Poll until dbctl config diff is clean."""
        ret = self.get_diff()
        if ret.success and ret.exit_code == 0:  # Empty diff
            return

        raise RuntimeError("dbctl config has a pending diff or unable to get the diff")

    def get_diff(self) -> ActionResult:
        """Get the current dbctl config diff."""
        ret, _ = self.dbctl.config.diff(datacenter=self.datacenter, force_unified=True)
        self.check_action_result(ret, "evaluate dbctl config diff")
        return ret
