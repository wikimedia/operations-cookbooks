"""Depool a DB from dbctl."""

import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from time import monotonic, sleep
from typing import Any

from conftool.extensions.dbconfig.action import ActionResult
from conftool.extensions.dbconfig.entities import Instance as DBCInst
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.mysql import ensure
from cookbooks.sre.mysql.pool import (
    extract_section_kind_and_method,
    fetch_host_instance_from_zarcillo,
    get_minst,
    get_mysqlremotehosts,
    validate_hostname_extract_dc_fqdn,
)
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.mysql import Instance as MInst
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

        hostname, _dc, fqdn = validate_hostname_extract_dc_fqdn(args.instance)

        self._mrhs = get_mysqlremotehosts(spicerack, fqdn)
        self._mysql_instance: MInst = get_minst(self._mrhs)

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
        timeout = monotonic() + 3600
        log.info("Monitoring number of wikiuser* connections")
        while monotonic() < timeout:
            wikiuser_cnt = _fetch_instance_connections_count_wikiusers(self._mysql_instance)
            if wikiuser_cnt == 0 or self.dry_run:
                log.info("Connection drain completed")
                return

            sleep(10)

        d = _fetch_instance_connections_count_detailed(self._mysql_instance)
        log.info("Drain timeout! Connection summary: %r", d)
        raise RuntimeError("The instance failed to drain in an hour")

    def _update_phabricator(self, status: str, desc: str) -> None:
        msg = f"{status} {desc} by {self.reason.owner}: {self.reason.reason}"
        if self.phabricator.task_accessible(self.task_id, raises=False):
            self.phabricator.task_comment(self.task_id, msg, raises=False)
        else:
            log.warning(f"Unable to access task {self.task_id}: not adding comment '{msg}'")

    def _depool_s_or_es(self) -> None:
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
            self._depool_s_or_es()
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
