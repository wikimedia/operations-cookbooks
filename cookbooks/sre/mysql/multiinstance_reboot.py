"""Safely reboot multi-instance MySQL hosts."""

# TODO: add cluster-wide soft locking
# TODO: add instance-level locking

import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta, timezone

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import (
    RemoteExecutionError,
    RemoteHosts,
)
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

log = logging.getLogger(__name__)

def step(slug: str, msg: str) -> None:
    """Logging helper."""
    log.info("[%s.%s] %s", __name__, slug, msg)


def _request_explicit_confirmation(fqdn: str):
    print(
        """This cookbook has only been tested on clouddb hosts. For other hosts,
        sre.mysql.upgrade is currently the preferred one.
        """
    )
    ask_confirmation(f"Are you sure you want to continue with {fqdn}?")


class MultiinstanceReboot(CookbookBase):
    """Safely reboots one or more multi-instance MySQL host(s).

    This cookbook can handle both multi-instance and single-instance MySQL
    hosts, but for single-instance hosts please use sre.mysql.upgrade instead,
    as it's more battle-tested. In the future we should consider merging these
    two cookbooks.

    If "query" matches more than one host, hosts are rebooted sequentially,
    one at a time.

    If --upgrade is provided, it also runs "apt-get dist-upgrade". This will
    install Debian updates and any patch-version updates of wmf-mariadb.

    Hosts are always depooled before rebooting. If --repool is provided, hosts
    will be repooled after the reboot, otherwise they will need to be repooled
    manually.

    Usage:
        cookbook sre.mysql.multiinstance_reboot "clouddb*"\
                --upgrade --repool --task-id T123456 --reason "Reboot all clouddbs"
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("query", help="Cumin query to match the host(s) to act upon.")
        parser.add_argument("--upgrade", action="store_true", help="Run apt-get dist-upgrade before reboot")
        parser.add_argument("--repool", action="store_true", help="Repool host after reboot")
        return parser

    def get_runner(self, args: Namespace) -> "MultiinstanceRebootRunner":
        """As specified by Spicerack API."""
        return MultiinstanceRebootRunner(args, self.spicerack)


def _fqdn(rhost: RemoteHosts) -> str:
    return tuple(rhost.hosts)[0]


class MultiinstanceRebootRunner(CookbookRunnerBase):
    """Upgrade MySQL cookbook runner."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Upgrade MySQL on a given set of hosts."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.alerting_hosts = spicerack.alerting_hosts
        self.icinga_hosts = spicerack.icinga_hosts
        self.remote = spicerack.remote()
        query = "P{" + args.query + "} and A:db-all"
        self.hosts: RemoteHosts = spicerack.remote().query(query)
        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)
        self._dbctl = spicerack.dbctl()
        self._phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self._mysql = spicerack.mysql()
        self._run_cookbook = spicerack.run_cookbook
        self._do_upgrade = args.upgrade
        self._do_repool = args.repool
        self.task_id = args.task_id
        self.admin_reason = spicerack.admin_reason(args.reason)
        self._confctl = spicerack.confctl("node")
        if not self.hosts:
            print("No hosts have been found, exiting")
        if len(self.hosts) <= 5:
            self.hosts_message = str(self.hosts)
        else:
            self.hosts_message = f"{len(self.hosts)} hosts"

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return f"for {self.hosts_message}"

    def _is_in_dbctl(self, hostname: str) -> bool:
        return self._dbctl.instance.get(hostname) is not None

    def _is_clouddb(self, hostname: str) -> bool:
        """Check if the hostname starts with 'clouddb'."""
        return hostname.startswith("clouddb")

    def _depool_clouddb(self, fqdn: str) -> None:
        """Depool a clouddb host using confctl."""
        step("depool_confctl", f"Depooling {fqdn} via confctl")
        confctl_services = list(self._confctl.filter_objects({}, name=fqdn))
        if not confctl_services:
            raise RuntimeError(f"No confctl objects found for {fqdn}")
        self._confctl.update_objects({"pooled": "no"}, confctl_services)

    def _repool_clouddb(self, fqdn: str) -> None:
        """Repool a clouddb host using confctl."""
        step("repool_confctl", f"Repooling {fqdn} via confctl")
        confctl_services = list(self._confctl.filter_objects({}, name=fqdn))
        if not confctl_services:
            raise RuntimeError(f"No confctl objects found for {fqdn}")
        self._confctl.update_objects({"pooled": "yes"}, confctl_services)

    def reboot_host(self, host):
        """Reboot a single host."""
        host_puppet = self.puppet(host)
        with self.alerting_hosts(host.hosts).downtimed(
                self.admin_reason,
                duration=timedelta(hours=24),
                remove_on_error=False,
            ):
            with host_puppet.disabled(self.admin_reason):
                self._run_reboot(host)

        # Pool in host after alerting goes green and is enabled, puppet is enabled, replication is in sync
        hostname = str(host).split(".", maxsplit=1)[0]
        reason = f"Reboot of {host} completed"
        if self._do_repool:
            if self._is_clouddb(hostname):
                self._repool_clouddb(_fqdn(host))
            elif self._is_in_dbctl(hostname):
                if self.task_id:
                    args = ["--reason", reason, "--task-id", self.task_id, hostname]
                else:
                    args = ["--reason", reason, hostname]

                self._run_cookbook("sre.mysql.pool", args, confirm=True)
        else:
            self.logger.info("Repooling not requested")

        self._phab.task_comment(self.task_id, reason)

    def run(self):
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)
        for host in self.hosts:
            self.reboot_host(host)

    def _run_reboot(self, host: RemoteHosts) -> None:
        """Reboot a single MySQL host."""
        fqdn = _fqdn(host)
        reason = f"Rebooting {host}"
        self._phab.task_comment(self.task_id, reason)

        hostname = str(host).split(".", maxsplit=1)[0]
        if not self._is_clouddb(hostname):
            _request_explicit_confirmation(fqdn)

        if self._is_clouddb(hostname):
            self._depool_clouddb(fqdn)
        elif self._is_in_dbctl(hostname):
            step("depool", f"Depooling {fqdn}")
            if self.task_id:
                args = ["--reason", reason, "--task-id", self.task_id, hostname]
            else:
                args = ["--reason", reason, hostname]

            self._run_cookbook("sre.mysql.depool", args, confirm=True)

        mysql_dbs = self._mysql.get_dbs(fqdn)
        instances = mysql_dbs.list_hosts_instances()

        for instance in instances:
            total_replicas = 0
            with instance.cursor(database="mysql") as (connection, cursor):
                if cursor.execute("SHOW REPLICA HOSTS"):
                    total_replicas = len(cursor.fetchall())

            if total_replicas > 1:
                msg = f"{instance} on {fqdn} appears to be a master with {total_replicas} replicas"
                log.error(msg)
                raise ValueError(msg)
            if total_replicas == 1:
                ask_confirmation("A single replica has been detected, proceed anyway?")

            step("stop_mariadb", f"Stopping MariaDB instance {instance} on {fqdn}")
            # TODO: use instance.stop_slave(), when that method will be
            # multiinstance-compatible
            instance.run_query("STOP SLAVE")
            instance.stop_mysql()

        if self._do_upgrade:
            step("apt_upgrade", f"Running apt-get dist-upgrade on {fqdn}")
            self.spicerack.apt_get(host).run("dist-upgrade")

        step("reboot", "Rebooting host")

        srv_needs_umount = False
        try:
            host.run_sync("mountpoint /srv")
            srv_needs_umount = True
        except RemoteExecutionError as err:
            for _, mt in err.results:
                if b'/srv is not a mountpoint' != mt.message():
                    self.logger.warning("Unexpected output: %s", mt)
                    raise
        else:
            if srv_needs_umount:
                self.logger.debug("Proceeding with /srv unmount")
                host.run_sync("umount /srv")

        host.run_sync("swapoff -a")
        reboot_time = datetime.now(timezone.utc)
        host.reboot()
        host.wait_reboot_since(reboot_time)

        # skip-slave-start should be the default, this is for extra safety
        host.run_sync('/usr/bin/systemctl set-environment MYSQLD_OPTS="--skip-slave-start"')

        for instance in instances:
            # TODO: use instance.resume_replication() to replace the steps
            # below, when that method will be multiinstance-compatible
            step("start_mariadb", f"Starting MariaDB instance {instance} on {fqdn}")
            instance.start_mysql()
            if self._do_upgrade:
                instance.upgrade()
                instance.restart_mysql()
            instance.run_query("START SLAVE")

            if instance.name:
                prom_service = f"prometheus-mysqld-exporter@{instance.name}.service"
            else:
                prom_service = "prometheus-mysqld-exporter.service"
            step("restart_prom_exp", f"Restarting Prometheus exporter for {instance}")
            host.run_sync(f"systemctl restart {prom_service}")

            # For multi-instance hosts, we don't want to wait for replication
            # before restarting the next instance. Instead, we can start
            # replication for all instances, then let them catch up in parallel.
            # "icinga_hosts.wait_for_optimal()" (called below) will ensure that
            # all instances are in sync.
            if not instance.name:
                step("wait_repl", f"Waiting for replication to catch up on {instance}")
                instance.wait_for_replication()

        step("wait_icinga_s", f"Waiting for icinga to go green for {fqdn}")
        self.icinga_hosts(host.hosts).wait_for_optimal()
