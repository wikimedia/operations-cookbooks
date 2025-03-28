"""Restart Sanitarium hosts"""

import logging
from argparse import ArgumentParser, Namespace
from time import sleep

from spicerack import Spicerack
from spicerack.mysql import MysqlRemoteHosts, Mysql
from spicerack.remote import RemoteHosts, Remote
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


# pylint: disable=missing-docstring
# pylint: disable=R0913,R0917
# flake8: noqa: D103

log = logging.getLogger(__name__)


def ensure(condition: bool, msg: str) -> None:
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def step(slug: str, msg: str) -> None:
    log.info("[%s.%s] %s", __name__, slug, msg)


def argument_parser() -> ArgumentParser:
    """Required by Spicerack."""
    ap = ArgumentParser(description=__doc__)
    ap.add_argument("-t", "--task-id", help="Phabricator task ID")
    ap.add_argument("--dc", help="Datacenter e.g. --dc=eqiad")
    ap.add_argument("--hostnames", nargs="*", help="Limit restarts: --hostnames db0000 db1234")
    return ap


def _restart_host(host: MysqlRemoteHosts, dryrun: bool, phab, task_id) -> None:
    host_instances = host.list_hosts_instances()
    for instance in host_instances:
        if phab:
            phabmsg = f"Restarting sanitarium instance {instance}"
            phab.task_comment(task_id, phabmsg)

        step("stop_repl", f"Running STOP SLAVE on {instance}")
        instance.run_query("STOP SLAVE", print_progress_bars=False)

        step("stop_mariadb", f"Stopping MariaDB on {instance}")
        instance.stop_mysql()

        step("start_mariadb", f"Starting MariaDB on {instance}")
        instance.start_mysql()

        step("start_repl", f"Starting replication on {instance}")
        instance.run_query("START SLAVE", print_progress_bars=False)

        if not dryrun:
            log.debug("Sleeping 1m")
            sleep(60)


def extract_mhosts(remote: Remote, mysql: Mysql, args: Namespace) -> list[MysqlRemoteHosts]:
    rh: RemoteHosts = remote.query("A:db-sanitarium")
    out = []
    for fqdn in rh.hosts:
        hn, dc, _tld = fqdn.split(".")
        if args.hostnames and hn not in args.hostnames:
            continue
        if args.dc and dc != args.dc:
            continue

        mhost = mysql.get_dbs(fqdn)
        out.append(mhost)

    return out


def run(args: Namespace, spicerack: Spicerack) -> None:
    """Required by Spicerack."""
    mysql = spicerack.mysql()
    remote = spicerack.remote()
    ensure_shell_is_durable()
    for hn in args.hostnames or ():
        ensure(hn.isalnum(), f"Invalid hostname {hn}")

    hosts_to_restart = extract_mhosts(remote, mysql, args)

    log.info("Provisional plan:")
    log.info("%-20s %-10s", "Hostname", "Instance count")
    hostnames = []
    for host in hosts_to_restart:
        cnt = len(host.list_hosts_instances())
        log.info("%-20s %-10s", host, cnt)
        hostnames.append(str(host))
        host_instances = host.list_hosts_instances()
        for instance in host_instances:
            instance.run_query("SELECT 1", is_safe=True, print_progress_bars=False)

    if spicerack.dry_run:
        print("--- dry run mode - not asking confirmation ---")
    else:
        ask_confirmation("Perform restarts?")

    if args.task_id:
        phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        phabmsg = "Restarting MariaDB instances on sanitarium host[s] "
        phabmsg += " ".join(hostnames)
        phab.task_comment(args.task_id, phabmsg)
    else:
        phab = None

    for host in hosts_to_restart:
        _restart_host(host, spicerack.dry_run, phab, args.task_id)

    if phab:
        phab.task_comment(args.task_id, phabmsg + ": completed")
