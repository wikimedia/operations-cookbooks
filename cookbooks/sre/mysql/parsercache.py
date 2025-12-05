"""Pool/depool parsercache hosts"""

import logging
import re
import sys
import time
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import Generator

from conftool.extensions.dbconfig.action import ActionResult
from spicerack import Spicerack
from spicerack.dbctl import Dbctl
from spicerack.icinga import IcingaHosts
from spicerack.remote import RemoteHosts

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


# pylint: disable=missing-docstring
# pylint: disable=R0913,R0917
# flake8: noqa: D103

log = logging.getLogger(__name__)


def ensure(condition: bool, msg: str) -> None:
    if not condition:
        log.error("Failed safety check: {msg}", exc_info=True)
        raise AssertionError(msg)


def step(slug: str, msg: str) -> None:
    log.info("[%s.%s] %s", __name__, slug, msg)


def check_section(section: str) -> str:
    match = re.fullmatch(r"(pc|ms)\d+", section)
    url = "https://wikitech.wikimedia.org/wiki/MariaDB/Troubleshooting#Depooling_a_parsercache_or_mainstash_host"
    ensure(match is not None, f"Invalid section, see {url}")
    return section


def argument_parser() -> ArgumentParser:
    """Required by Spicerack."""
    ap = ArgumentParser(description=__doc__)
    ap.add_argument("section", help="Section name e.g. pc3", type=check_section)
    ap.add_argument("-t", "--task-id", help="Phabricator task ID")
    ap.add_argument("-r", "--reason", help="Reason")
    subs = ap.add_subparsers(dest="action", required=True, help="Action to perform")
    subs.add_parser("show", help="Show")

    pool = subs.add_parser("pool", help="Pool")
    pool.add_argument("--skip-icinga-checks", action="store_true", help="Skip checks before pooling")

    depool = subs.add_parser("depool", help="Depool")
    depool.add_argument("--downtime-hours", type=int, help="Create downtime (default: no)", default=0)
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


def update_phabricator(hns: list, args: Namespace, spicerack: Spicerack, reason, summary: str) -> None:
    if hns and args.task_id:
        phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        names = " and ".join(sorted(hns))
        log.info("Updating Phabricator")
        phab.task_comment(args.task_id, f"Depooled {names} {reason}")


def _get_dbctl_config_diff(dbctl: Dbctl) -> tuple[bool, Generator]:
    """Return True for changes and a diff line generator"""
    for attempt in range(5):
        ret, diff = dbctl.config.diff(force_unified=False)
        if ret.success:
            has_changes = bool(ret.exit_code)
            return (has_changes, diff)
        time.sleep(5)

    raise RuntimeError("Unable to run `dbctl config diff` %s", ret)


def set_weight_in_dbctl(dbctl: Dbctl, reason, fqdns: list, weight: int) -> bool:
    """Set weight in dbctl and commit"""
    _wait_for_dbctl_diff_empty(dbctl)
    for fqdn in fqdns:
        hn, dc, _tld = fqdn.split(".")

        for attempt in range(5):
            step("pool", f"Setting weight for {hn} to {weight}")
            res = dbctl.instance.weight(hn, weight)
            _log_dbctl_result(res)
            if res.success:
                break
            time.sleep(10)

        else:
            log.error("Failed to update, exiting immediately")
            has_changes, _ = _get_dbctl_config_diff(dbctl)
            if has_changes:
                # TODO: try rolling back any change if possible
                log.error("`dbctl config diff` is not clean!")
            sys.exit(1)

    has_changes, diff = _get_dbctl_config_diff(dbctl)
    if not has_changes:
        return False

    log.info("Changes:")
    for row in diff:
        log.debug(row.rstrip())
    log.info("Committing dbctl config")
    ret = dbctl.config.commit(batch=True, comment=reason.reason)
    _log_dbctl_result(ret)
    return True


def depool(spicerack: Spicerack, args: Namespace, alerting_hosts: IcingaHosts, dbctl: Dbctl, fqdns: list) -> None:
    reason = spicerack.admin_reason(args.reason or "Depooling", args.task_id)
    fq = ", ".join(sorted(fqdns))
    desc = f"depool all hosts in '{args.section}': {fq}"
    log.info(f"Preparing to {desc}")

    if getattr(args, "downtime_hours", 0):
        step("pool", "Setting Icinga downtime")
        alerting_hosts.downtime(reason, duration=timedelta(hours=args.downtime_hours))

    changed = set_weight_in_dbctl(dbctl, reason, fqdns, 0)

    if changed:
        update_phabricator(fqdns, args, spicerack, reason, "Depooled")
        log.info(f"Completed {desc}")
    else:
        log.info("No changes to dbctl were made. Perhaps the hosts were already depooled?")


def pool(spicerack: Spicerack, args: Namespace, alerting_hosts: IcingaHosts, dbctl: Dbctl, fqdns: list) -> None:
    reason = spicerack.admin_reason(args.reason or "Pooling", args.task_id)
    fq = ", ".join(sorted(fqdns))
    desc = f"pool all hosts in '{args.section}': {fq}"
    log.info(f"Preparing to {desc}")

    run_icinga_checks = not getattr(args, "skip_icinga_checks", False)
    if run_icinga_checks:
        step("pool", "Rechecking and waiting for Icinga to be green")
        alerting_hosts.recheck_all_services()
        alerting_hosts.wait_for_optimal(skip_acked=False)

        step("pool", "Removing Icinga downtime if any")
        alerting_hosts.remove_downtime()

    changed = set_weight_in_dbctl(dbctl, reason, fqdns, 1)

    if changed:
        update_phabricator(fqdns, args, spicerack, reason, "Pooled")
        log.info(f"Completed {desc}")
    else:
        log.info("No changes to dbctl were made. Perhaps the hosts were already pooled in?")


def run(args: Namespace, spicerack: Spicerack) -> None:
    """Required by Spicerack."""
    dbctl = spicerack.dbctl()
    query = "A:db-section-" + args.section
    hosts: RemoteHosts = spicerack.remote().query(query)
    fqdns = list(hosts.hosts)
    log.info("Hosts found: %s", " ".join(sorted(fqdns)))
    ensure(len(fqdns) == 2, f"2 hosts expected, found: {fqdns}")

    alerting_hosts = spicerack.icinga_hosts(fqdns)

    if args.action == "show":
        return

    if args.action == "depool":
        depool(spicerack, args, alerting_hosts, dbctl, fqdns)

    elif args.action == "pool":
        pool(spicerack, args, alerting_hosts, dbctl, fqdns)
