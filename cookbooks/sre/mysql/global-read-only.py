"""
Set/unset sections as read-only in dbctl and on MariaDB.

The tool is idempotent. It makes changes on dbctl (if needed), then
runs `SET GLOBAL read_only=...` on MariaDB and updates Phabricator.
"""

import sys
import time
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser, Namespace
from logging import Logger, getLogger
from typing import Generator

from conftool.extensions.dbconfig.action import ActionResult
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from spicerack import Spicerack
from spicerack.dbctl import Dbctl
from spicerack.mysql import Mysql
from wmflib.interactive import ask_confirmation

log = getLogger(__name__)

DEFAULT_SECTIONS = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"]


def ensure(condition: bool, msg: str) -> None:
    if not condition:
        log.error(f"Failed safety check: {msg}", exc_info=True)
        raise AssertionError(msg)


def step(slug: str, msg: str) -> None:
    log.info("[%s.%s] %s", __name__, slug, msg)


def argument_parser() -> ArgumentParser:
    """Required by Spicerack."""
    ap = ArgumentParser(description=__doc__, formatter_class=ArgumentDefaultsHelpFormatter)
    h = """action to perform:
    set-ro:          sets all sections as read-only
    unset-ro|set-rw: set all sections as read-write"""
    ap.add_argument("action", help=h, choices=["set-ro", "unset-ro", "set-rw"])
    ap.add_argument(
        "--ignore-dirty-dbctl",
        action="store_true",
        help="Do not wait for dbctl config diff to be clean: force the change",
    )
    ap.add_argument("-t", "--task-id", help="Phabricator task ID")
    ap.add_argument("-r", "--reason", help="Reason")
    ds = ",".join(DEFAULT_SECTIONS)
    ap.add_argument("--sections", help="Comma-separated section names", default=ds)
    return ap


def _wait_for_dbctl_diff_empty(dbctl: Dbctl) -> None:
    step("dbctl", "Waiting for dbctl diff to be empty")
    for retry in range(100):
        has_changes, _ = _get_dbctl_config_diff(dbctl)
        if not has_changes:
            return

        log.debug("Attempt %d to get clean dbctl config diff", retry)
        time.sleep(5)

    log.error("Timed out while waiting for dbctl config diff to be empty")
    sys.exit(1)


def _log_dbctl_result(res: ActionResult) -> None:
    for msg in res.messages:
        log.info(msg) if res.success else log.error(msg)

    if res.announce_message:
        log.info(res.announce_message)


def update_phabricator(
    args: Namespace, spicerack: Spicerack, msg: str, sections: list[str], done_mariadb_sections: list[str]
) -> None:
    if not args.task_id:
        return

    phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    if done_mariadb_sections != sections:
        msg += "\nNot all MariaDB masters were updated successfully:"
        msg += f"\nTODO: {sections}"
        msg += f"\nDONE: {done_mariadb_sections}"

    if phab.task_accessible(args.task_id, raises=False):
        log.info("Updating Phabricator")
        phab.task_comment(args.task_id, msg, raises=False)

    else:
        log.warning(f"Unable to access task {args.task_id}: not adding comment '{msg}'")


def _get_dbctl_config_diff(dbctl: Dbctl) -> tuple[bool, Generator]:
    """Return True for changes and a diff line generator"""
    for attempt in range(5):
        ret, diff = dbctl.config.diff(force_unified=False)
        if ret.success:
            has_changes = bool(ret.exit_code)
            return (has_changes, diff)
        time.sleep(5)

    raise RuntimeError(f"Unable to run `dbctl config diff` {ret}")


def _prepare_dbctl_changes(sections: list[str], dbctl: Dbctl, readonly_flag: bool) -> None:
    for sec in sections:
        for dc in ["codfw", "eqiad"]:
            for attempt in range(20):
                res = dbctl.section.set_readonly(sec, dc, readonly_flag)
                _log_dbctl_result(res)
                if res.success:
                    log.debug(f"Setting dbctl {sec} in {dc}")
                    break
                log.debug(f"Failed to set {sec} in {dc}, waiting 5s")
                time.sleep(5)

            else:
                log.error("Failed to update, exiting immediately. Remember to clean up dbctl if needed.")
                sys.exit(1)


def _format_msg(args: Namespace, sections: list[str], readonly_flag: bool) -> str:
    s = ", ".join(sections)
    msg = f"Setting sections {s} as "
    if readonly_flag:
        msg += "read-only"
    else:
        msg += "read-write"

    if args.task_id:
        msg += f" for {args.task_id}"

    if args.reason:
        msg += f": '{args.reason}'"

    return msg


def _set_global_on_mariadb(sections: list[str], dc: str, mysql: Mysql, readonly_flag: bool) -> list[str]:
    """Run SET GLOBAL read_only=... on MariaDB primary masters.
    If it fails we log the error, continue, and return the list of successfully changed sections.
    """
    done_sections = []
    for sec in sections:
        try:
            mrhs = mysql.get_core_dbs(datacenter=dc, section=sec, replication_role="master")
            insts = mrhs.list_hosts_instances()
            if len(insts) != 1:
                raise Exception(f"Expected 1 instance, found {len(insts)}")

            inst = insts[0]
            if readonly_flag:
                log.info(f"Setting MariaDB {sec} in {dc} read-only")
                inst.run_query("SET GLOBAL read_only=1")
            else:
                log.info(f"Setting MariaDB {sec} in {dc} read-write")
                inst.run_query("SET GLOBAL read_only=0")

            done_sections.append(sec)

        except Exception as e:
            log.error(f"Error {sec} {e}")

    return done_sections


def _update_dbctl(dbctl: Dbctl, sal_log: Logger, sections: list[str], readonly_flag: bool, msg: str) -> None:
    # First prepare a dbctl change for all sections and DCs without committing
    _prepare_dbctl_changes(sections, dbctl, readonly_flag)

    # Check if we made actual changes in dbctl
    has_changes, diff = _get_dbctl_config_diff(dbctl)
    if has_changes:
        log.info("Changes:")
        for row in diff:
            log.debug(row.rstrip())

        log.info(f"Committing dbctl config: {msg}")
        sal_log.info(f"Dbctl change: {msg}")
        ret = dbctl.config.commit(batch=True, comment=msg)
        _log_dbctl_result(ret)

    else:
        log.info("No changes needed on dbctl")


def run(args: Namespace, spicerack: Spicerack) -> None:
    """Required by Spicerack."""
    readonly_flag = args.action == "set-ro"
    sections = args.sections.split(",")
    sections = sorted(s.strip() for s in sections)

    msg = _format_msg(args, sections, readonly_flag)

    dbctl = spicerack.dbctl()
    mysql = spicerack.mysql()
    sal_log = spicerack.sal_logger

    primary_dc = spicerack.mediawiki().get_master_datacenter()
    log.info(f"Primary DC: {primary_dc}")

    if not args.ignore_dirty_dbctl:
        _wait_for_dbctl_diff_empty(dbctl)

    ask_confirmation(f"CAUTION: {msg} - are you really sure?")

    if readonly_flag:
        log.info("Going read-only: first dbctl then MariaDB")
        _update_dbctl(dbctl, sal_log, sections, readonly_flag, msg)

        # Sleep to let MW pick up the change
        time.sleep(5)

        sal_log.info(f"MariaDB change: {msg}")
        # dbctl has already been updated to RO. If we fail to set any primary master here we just
        # log it in stdout and in Phabricator
        done_mariadb_sections = _set_global_on_mariadb(sections, primary_dc, mysql, readonly_flag)

    else:
        # We first set primary masters in RW, gather which sections had the change applied,
        # then switch dbctl to RW only where MariaDB is RW
        log.info("Going read-write: first MariaDB then dbctl")
        sal_log.info(f"MariaDB change: {msg}")
        done_mariadb_sections = _set_global_on_mariadb(sections, primary_dc, mysql, readonly_flag)

        if done_mariadb_sections != sections:
            log.error("Not all MariaDB masters were switched to read-write successfully!")
            log.error(f"TODO: {sections}")
            log.error(f"DONE: {done_mariadb_sections}")
            log.info("dbctl will be set to read-write only for the MariaDB masters that have been switched")

        _update_dbctl(dbctl, sal_log, done_mariadb_sections, readonly_flag, msg)

    update_phabricator(args, spicerack, msg, sections, done_mariadb_sections)
