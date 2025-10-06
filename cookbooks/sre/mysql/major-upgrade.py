"""Migrate to a different major version of MariaDB hosts."""

# TODO: add cluster-wide soft locking
# TODO: add instance-level locking

import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta
from time import sleep

from spicerack import Spicerack
from spicerack.mysql import Instance as MInst, Mysql
from spicerack.remote import RemoteHosts
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable, ask_confirmation

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


log = logging.getLogger(__name__)

# pylint: disable=missing-docstring,logging-fstring-interpolation


def step(slug: str, msg: str) -> None:
    """Logging helper."""
    log.info("[%s.%s] %s", __name__, slug, msg)


def get_db_instance(mysql: Mysql, fqdn: str) -> MInst:
    """Get Mysql Instance."""
    db = mysql.get_dbs(fqdn)
    return db.list_hosts_instances()[0]


def argument_parser() -> ArgumentParser:
    """Required by Spicerack."""
    ap = ArgumentParser(description=__doc__)
    ap.add_argument("hostname", help="Hostname e.g. db1234")
    ap.add_argument("old_pkg", help="Old MariaDB Debian package e.g. wmf-mariadb106")
    ap.add_argument("-t", "--task-id", help="Phabricator task ID")
    ap.add_argument("--repool", action="store_true", help="Pool in host after upgrade")
    return ap


def pool(run_cookbook, hostname, task_id, reason) -> None:
    """Pool a host back."""
    step("pool", f"Pooling {hostname}")
    if task_id:
        cbargs = ["--reason", reason, "--task-id", task_id, hostname]
    else:
        cbargs = ["--reason", reason, hostname]
    run_cookbook("sre.mysql.pool", cbargs, confirm=True)


def depool(run_cookbook, hostname, task_id, reason) -> None:
    """Depool a host."""
    step("depool", f"Depooling {hostname}")
    if task_id:
        cbargs = ["--reason", reason, "--task-id", task_id, hostname]
    else:
        cbargs = ["--reason", reason, hostname]
    run_cookbook("sre.mysql.depool", cbargs, confirm=True)


def _fqdn(rhost: RemoteHosts) -> str:
    return tuple(rhost.hosts)[0]


def is_in_dbctl(dbctl, hostname: str) -> bool:
    """Check if hostname is in dbctl."""
    return dbctl.instance.get(hostname) is not None


def run_scripts(host: RemoteHosts, scripts: list[str]) -> None:
    """Run a list of scripts on a remote host."""
    for script in scripts:
        try:
            confirm_on_failure(host.run_sync, script)
        except AbortError:
            log.error("%s: execution aborted", script)
            raise


def run_apt_get(host, cmd: str) -> None:
    """Run apt-get command on remote host."""
    scripts = [
        f"DEBIAN_FRONTEND=noninteractive apt-get {cmd}",
    ]
    run_scripts(host, scripts)


def run_upgrade(  # pylint: disable=too-many-arguments,too-many-positional-arguments
    spicerack,
    host: RemoteHosts,
    phab,
    run_cookbook,
    admin_reason,
    task_id: str,
    do_repool: bool,
    old_pkg: str,
) -> None:
    """Migrate MariaDB version of a single host."""
    fqdn = _fqdn(host)
    reason = f"Upgrading {host}"
    if task_id:
        phab.task_comment(task_id, reason)

    hostname = str(host).split(".", maxsplit=1)[0]
    if is_in_dbctl(spicerack.dbctl(), hostname):
        depool(run_cookbook, hostname, task_id, reason)

    msg = "Create a patch to puppet to configure the new MariaDB pkg version. Do not merge it yet."
    ask_confirmation(msg)

    # TODO check puppet https://phabricator.wikimedia.org/T389663

    step("stop_puppet", "Disabling Puppet")
    host_puppet = spicerack.puppet(host)
    host_puppet.disable(admin_reason)

    step("stop_mariadb", f"Stopping mariadb on {fqdn}")
    scripts = [
        'mysql -e "stop slave;"',
        "systemctl stop mariadb",
    ]
    run_scripts(host, scripts)

    step("remove_pkg", f"Removing old MariaDB package {old_pkg}")
    # Puppet is not being used to remove packages
    run_apt_get(host, f"remove {old_pkg}")

    msg = "Finalize and puppet-merge the patch to Puppet."
    ask_confirmation(msg)

    # This will install the new package gets installed
    step("start_run_puppet", "Starting puppet and wait for it to run")
    host_puppet.enable(admin_reason)
    host_puppet.wait()

    # TODO ensure MariaDB is installed?

    step("apt_update", "Running apt-get update")
    run_apt_get(host, "update")

    step("apt_upgrade", "Running apt-get dist-upgrade")
    upgrade_cmd = "-y -o Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold' dist-upgrade"
    run_apt_get(host, upgrade_cmd)

    reboot(host)

    start_mariadb_and_run_mysql_upgrade(host)

    sleep(10)
    step("restart_prom_exp", "Restarting Prometheus exporter")
    scripts = ["systemctl restart prometheus-mysqld-exporter.service"]
    run_scripts(host, scripts)

    reason = f"Migrate of {host} completed"
    if not do_repool:
        log.info("Repooling not requested")
        if task_id:
            phab.task_comment(task_id, reason)
        return

    step("catchup_repl_s", f"Catching up replication lag on {fqdn} before removing icinga downtime")
    get_db_instance(spicerack.mysql(), fqdn).wait_for_replication()

    step("wait_icinga_s", f"Waiting for icinga to go green for {fqdn}")
    spicerack.icinga_hosts(host.hosts).wait_for_optimal()


def start_mariadb_and_run_mysql_upgrade(host):
    """Start MariaDB and run mysql_upgrade."""
    step("mysql_upgrade", "Start MariaDB and run mysql_upgrade")
    scripts = [
        'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
        "systemctl start mariadb",
        "mysql_upgrade",
        "systemctl restart mariadb",
        'mysql -e "start slave;"',
    ]
    run_scripts(host, scripts)


def reboot(host):
    """Reboot the host."""
    step("reboot", "Rebooting host")
    scripts = [
        "umount /srv",
        "swapoff -a",
    ]
    run_scripts(host, scripts)
    reboot_time = datetime.utcnow()
    host.reboot()
    host.wait_reboot_since(reboot_time)


def run(args: Namespace, spicerack: Spicerack) -> None:
    """Required by Spicerack."""
    ensure_shell_is_durable()

    # Guard against useless conftool messages
    logging.getLogger("conftool").setLevel(logging.WARNING)

    phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
    admin_reason = spicerack.admin_reason(f"Migrate MariaDB on {args.hostname}")

    host = spicerack.remote().query(args.hostname)
    if not host:
        print(f"Host {args.hostname} not found, exiting")
        return

    run_cookbook = spicerack.run_cookbook
    task_id = args.task_id

    with spicerack.alerting_hosts(host.hosts).downtimed(admin_reason, duration=timedelta(hours=24)):
        run_upgrade(
            spicerack,
            host,
            phab,
            run_cookbook,
            admin_reason,
            task_id,
            args.repool,
            args.old_pkg,
        )

    hostname = str(host).split(".", maxsplit=1)[0]
    reason = f"Migration of {host} completed"
    if args.repool and is_in_dbctl(spicerack.dbctl(), hostname):
        pool(run_cookbook, hostname, task_id, reason)

    if task_id:
        phab.task_comment(task_id, reason)
