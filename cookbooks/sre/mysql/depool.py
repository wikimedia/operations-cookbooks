"""Depool a DB from dbctl."""

import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from time import monotonic, sleep
from typing import Any

from pymysql.err import OperationalError

from conftool.extensions.dbconfig.action import ActionResult
from conftool.extensions.dbconfig.entities import Instance as DBCInst
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.mysql import ZarcilloClient, ensure
from cookbooks.sre.mysql.pool import (
    InstanceMetadata,
    dbctl_set_section_ro_or_rw,
    extract_section_kind_and_method,
    fetch_host_instance_from_zarcillo,
    get_minst,
    get_mysqlremotehosts,
    is_es_section_rw,
    validate_hostname_extract_dc_fqdn,
)
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.mysql import Instance as MInst
from spicerack.remote import RemoteExecutionError
from wmflib.interactive import ask_confirmation

log = logging.getLogger(__name__)


def step(slug: str, msg: str) -> None:
    """Logging helper."""
    log.info("[%s.%s] %s", __name__, slug, msg)


def _check_depooling_last_instance(conf: dict[str, Any], hostname: str, nocheck_extloads: bool) -> None:
    """Warn if removing the only host in a section (e.g. vslow or dump)."""
    ensure("." not in hostname, f"hostname '{hostname}' contains a dot")

    for dc, dc_conf in conf.items():
        if nocheck_extloads is False:
            ext_loads = dc_conf["externalLoads"]
            for section, li in ext_loads.items():
                for d in li:
                    if len(d) == 1 and hostname in d:
                        print(f"{hostname} is the only entry in dc: {dc} section: {section}")
                        ask_confirmation("CAUTION: attempting to depool the only instance in a section!")

        group_loads = dc_conf["groupLoadsBySection"]
        for section, group_d in group_loads.items():
            for group, d in group_d.items():
                if len(d) == 1 and hostname in d:
                    print(f"{hostname} is the only entry in dc: {dc} section: {section} group: {group}")
                    ask_confirmation("CAUTION: attempting to depool the only instance in a section!")


def _fetch_instance_connections_count_wikiusers(ins: MInst) -> int:
    """Count database instance connections matching wiki-related users."""
    sql = "SELECT COUNT(*) AS cnt FROM information_schema.processlist WHERE user LIKE '%%wiki%%'"
    row = ins.fetch_one_row(sql, ())
    return int(row["cnt"])


def _fetchall(ins: MInst, sql: str, args: tuple) -> tuple[dict]:
    with ins.cursor() as (_conn, cur):
        _ = cur.execute(sql, args)
        res = tuple(cur.fetchall())
        ins.check_warnings(cur)
        return res


def _fetch_instance_connections_count_detailed(ins: MInst) -> tuple[dict[str, Any]]:
    """Gather database instance connection counts.

    +----------+-----------------+-----------+
    | count(*) | user            | command   |
    +----------+-----------------+-----------+
    |        1 | cumin2024       | Query     |
    |        1 | event_scheduler | Daemon    |
    |        3 | orchestrator    | Sleep     |
    |        1 | system user     | Slave_IO  |
    |        1 | system user     | Slave_SQL |
    |       27 | wikiuser2023    | Sleep     |
    +----------+-----------------+-----------+
    """
    sql = """SELECT user, command, COUNT(*) AS cnt
        FROM information_schema.processlist GROUP BY user, command"""
    return _fetchall(ins, sql, ())


class Depool(CookbookBase):
    """Depool a DB instance from dbctl.

    Examples:
        # Immediately depool the instance
        sre.mysql.newdepool -r "Some reason" db1001

        # Immediately depool the instance and update a Phabricator task
        sre.mysql.newdepool -r "Some reason" -t T12345 db1001

    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--nocheck-external-loads",
            action="store_true",
            help="Disable safety check that prevents depooling the only host in externalLoads",
        )
        parser.add_argument("--downtime", type=int, help="Add downtime in hours")

        # TODO: add support for multiple instances? Based on what? (puppetdb, dbctl, orchestrator)
        parser.add_argument("instance", help="Hostname or FQDN")

        return parser

    def get_runner(self, args: Namespace) -> "DepoolRunner":
        """As specified by Spicerack API."""
        return DepoolRunner(args, self.spicerack)


class DepoolRunner(CookbookRunnerBase):
    """Depool a MySQL instance cookbook runner."""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """As specified by Spicerack API."""
        # Silence some more noisy loggers for the dry-run mode
        logging.getLogger("etcd.client").setLevel(logging.INFO)
        logging.getLogger("conftool").setLevel(logging.INFO)

        self.args = args
        self.dbctl = spicerack.dbctl()
        self.downtime = self.args.downtime
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
        self.task_id = args.task_id
        self.dry_run = spicerack.dry_run
        self._mysql = spicerack.mysql()
        self._run_cookbook = spicerack.run_cookbook
        self._zarcillo_client = ZarcilloClient(spicerack)

        hostname, _dc, fqdn = validate_hostname_extract_dc_fqdn(args.instance)

        self._mrhs = get_mysqlremotehosts(spicerack, fqdn)

        dbi: DBCInst = self.dbctl.instance.get(hostname)
        ensure(dbi is not None, f"Unable to find instance {hostname} in dbctl. Aborting.")
        ensure(dbi.name == hostname, f"Incorrect host found {dbi.name} vs {hostname}")
        self._hostname = hostname

        self.datacenter = dbi.tags.get("datacenter")

        nodeset = self._mrhs.remote_hosts.hosts
        self._icinga_host = spicerack.icinga_hosts(nodeset)
        self._alerting_hosts = spicerack.alerting_hosts(self._mrhs.remote_hosts.hosts)

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return f"depool {self.args.instance}: {self.reason.reason}"

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-instance."""
        return LockArgs(suffix=self.args.instance, concurrency=1, ttl=60)

    def wait_for_connection_drain(self) -> None:
        """Wait for connections from the parser to drain.

        NOTE: this does not support misc databases
        """
        try:
            minst: MInst = get_minst(self._mrhs)
        except RemoteExecutionError:
            log.error("Failed to list instances on the host: the host is probably unreachable.")
            log.info("Skipping the monitoring of wikiuser* connections. The depooling is done.")
            return

        timeout = monotonic() + 3600
        log.info("Monitoring number of wikiuser* connections")
        while monotonic() < timeout:
            for attempt in range(3):
                try:
                    wikiuser_cnt = _fetch_instance_connections_count_wikiusers(minst)
                    log.debug("Found %d connection(s), checking count", wikiuser_cnt)
                    break
                except OperationalError as err:
                    if attempt == 2:
                        ask_confirmation("CAUTION: we failed to check the connections 3 times, proceed anyway?")
                        return
                    else:
                        log.warning("Failed to connect to the database on attempt %d: %s", attempt, err)
                        sleep(1)
            if wikiuser_cnt == 0 or self.dry_run:
                log.info("Connection drain completed")
                return
            sleep(10)

        d = _fetch_instance_connections_count_detailed(minst)
        log.info("Drain timeout! Connection summary: %r", d)
        raise RuntimeError("The instance failed to drain in an hour")

    def _update_phabricator(self, status: str, desc: str) -> None:
        msg = f"{status} {desc} by {self.reason.owner}: {self.reason.reason}"
        if self.phabricator.task_accessible(self.task_id, raises=False):
            self.phabricator.task_comment(self.task_id, msg, raises=False)
        else:
            log.warning(f"Unable to access task {self.task_id}: not adding comment '{msg}'")

    def _depool_s(self) -> None:
        msg = "depool instance {self.args.instance}"
        if self.downtime:
            step("depool", "Setting downtime")
            self._alerting_hosts.downtime(self.reason, duration=timedelta(hours=self.downtime))

        self.wait_diff_clean()

        ar, dbctl_conf = self.dbctl.config.generate()
        self.check_action_result(ar, "Failed to generate dbctl conf")
        _check_depooling_last_instance(dbctl_conf, self.args.instance, self.args.nocheck_external_loads)
        ret = self.dbctl.instance.depool(self.args.instance)
        self.check_action_result(ret, msg)
        self.commit_change(msg)

        self.wait_for_connection_drain()

    def _depool_es(self, imeta: InstanceMetadata, section: str) -> str:
        """Depool esX host, see T430769

        Returns a msg describing the action being taken

        | Role    | Section  | Action         |
        | replica | *        | depool         |
        | master  | RO       | switchover     |
        | master  | RW       | set section RO |
        """
        if imeta.role in ["rep", "replica"]:
            # regardless of being a RO or RW sections, just depool the replica

            msg = f"depool {section} replica {self.args.instance}"

            if self.downtime:
                step("depool", "Setting downtime")
                self._alerting_hosts.downtime(self.reason, duration=timedelta(hours=self.downtime))

            self.wait_diff_clean()

            ar, dbctl_conf = self.dbctl.config.generate()
            self.check_action_result(ar, "Failed to generate dbctl conf")
            _check_depooling_last_instance(dbctl_conf, self.args.instance, self.args.nocheck_external_loads)
            ret = self.dbctl.instance.depool(self.args.instance)
            self.check_action_result(ret, msg)
            self.commit_change(msg)

            self.wait_for_connection_drain()
            return msg

        # we are depooling a RW master or RO fake-master

        is_rw_section = is_es_section_rw(section)
        log.debug(f"is_rw_section: {is_rw_section}")

        if is_rw_section:
            # Depooling a real master in a read-write section: we cannot alter the replication topology
            # so set the whole section as read-only.
            msg = (
                f"{self.args.instance} is a primary or DC master of {section} (RW): setting whole section as read-only!"
            )
            log.warning(msg)

            if self.downtime:
                log.warning(f"Setting a downtime on {self.args.instance}")
                self._alerting_hosts.downtime(self.reason, duration=timedelta(hours=self.downtime))

            self.wait_diff_clean()
            dbctl_set_section_ro_or_rw(self.dbctl, section, True)
            self.commit_change(msg)
            return f"setting read-write section {section} as read-only"

        # Depooling a "fake-master" in a read-only section: we can safely pick a healthy replica in the same DC
        # and do a "fake" switchover in dbctl
        log.warning(f"The fake-master {self.args.instance} in RO section {section} is being depooled")
        log.warning("doing a master/replica switchover")

        # Look for a replica in the same DC to switchover to

        section_status = self._zarcillo_client.fetch_section_status(section)
        instances = [i for i in section_status.instances if i.dc == imeta.dc]
        preferred_candidates = [i for i in instances if i.role != "master" and i.preferred_candidate]
        if len(preferred_candidates) != 1:
            raise ValueError(f"Expected one preferred candidate, found {preferred_candidates}")

        new_master = preferred_candidates[0]
        log.info(f"Found {new_master.hostname} as preferred candidate")

        if self.downtime:
            step("depool", "Setting downtime")
            self._alerting_hosts.downtime(self.reason, duration=timedelta(hours=self.downtime))

        msg = f"fake-switchover from {self.args.instance} to new master {new_master.hostname}"
        self.wait_diff_clean()
        dbc_new_master: DBCInst = self.dbctl.instance.get(new_master.hostname, dc=new_master.dc)
        if dbc_new_master is None:
            raise RuntimeError(f"Unable to find dbctl entity for {new_master.hostname} in {new_master.dc}")

        ret = self.dbctl.section.set_master(section, new_master.dc, dbc_new_master)
        self.check_action_result(ret, msg)
        ret = self.dbctl.instance.depool(self.args.instance)
        self.check_action_result(ret, msg)
        self.commit_change(msg)
        return msg

    def _depool_pc_or_ms(self, section: str) -> None:
        cmar = []
        if self.args.reason:
            cmar.extend(["--reason", self.args.reason])

        if self.args.task_id:
            cmar.extend(["--task-id", self.args.task_id])

        if self.downtime:
            cmar.extend(["--downtime", self.downtime])

        cmar.extend([section, "depool"])
        self._run_cookbook("sre.mysql.parsercache", cmar)

    def run(self) -> None:
        """As required by the Spicerack API."""
        try:
            imeta = fetch_host_instance_from_zarcillo(self.args.instance)
            section = imeta.section
        except Exception as e:
            imeta = None
            log.error(f"Error {e}")
            log.info("If you want to continue anyway input the section: ")
            section = input("Section: ").strip().lower()

        _, pool_method = extract_section_kind_and_method(section)

        if pool_method == "pc":
            log.info("Using parsercache cookbook")
            log.info(f"The whole '{section}' section will be depooled")
            self._depool_pc_or_ms(section)

            # currently parsercache cookbook does its own phab updating
            # self._update_phabricator("Completed", msg)

        elif pool_method == "s":
            msg = f"depooling of {self.args.instance}"
            self._depool_s()
            self._update_phabricator("Completed", msg)

        elif pool_method == "es":
            msg = f"depooling of {self.args.instance}"
            if imeta is None:
                raise ValueError("Depooling es sections requires InstanceMetadata from Zarcillo")

            msg = self._depool_es(imeta, section)
            self._update_phabricator("Completed", msg)

    # # dbctl related # #

    def check_action_result(self, action_result: ActionResult, message: str) -> None:
        """Raise on failure and log any messages present in an ActionResult instance."""
        for result_message in action_result.messages:
            log.log(logging.INFO if action_result.success else logging.ERROR, result_message)

        if action_result.announce_message:
            log.info(action_result.announce_message)

        if not action_result.success:
            raise RuntimeError(f"Failed to {message}")

    def commit_change(self, message: str) -> None:
        """Check the diff and commit the change."""
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
