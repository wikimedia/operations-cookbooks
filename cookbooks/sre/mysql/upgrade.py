"""Upgrade minor version of MySQL hosts."""

# TODO: add cluster-wide soft locking
# TODO: add instance-level locking

import logging
from argparse import ArgumentParser, Namespace
from datetime import datetime, timedelta

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.mysql import Instance as MInst, Mysql
from spicerack.remote import RemoteHosts
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


log = logging.getLogger(__name__)

# pylint: disable=missing-docstring,logging-fstring-interpolation,too-many-instance-attributes


def step(slug: str, msg: str) -> None:
    """Logging helper."""
    log.info("[%s.%s] %s", __name__, slug, msg)


def get_db_instance(mysql: Mysql, fqdn: str) -> MInst:
    """Get Mysql Instance."""
    db = mysql.get_dbs(fqdn)
    return db.list_hosts_instances()[0]


class UpgradeMySQL(CookbookBase):
    """Upgrade minor veresion of MySQL hosts."""

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("query", help="Cumin query to match the host(s) to act upon.")

        parser.add_argument("--repool", action="store_true", help="Pool in host after upgrade")
        return parser

    def get_runner(self, args: Namespace) -> "UpgradeMySQLRunner":
        """As specified by Spicerack API."""
        return UpgradeMySQLRunner(args, self.spicerack)


def _fqdn(rhost: RemoteHosts) -> str:
    return tuple(rhost.hosts)[0]


class UpgradeMySQLRunner(CookbookRunnerBase):
    """Upgrade MySQL cookbook runner."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Upgrade MySQL on a given set of hosts."""
        ensure_shell_is_durable()

        self.alerting_hosts = spicerack.alerting_hosts
        self.icinga_hosts = spicerack.icinga_hosts
        self.remote = spicerack.remote()
        query = "P{" + args.query + "} and A:db-all and not A:db-multiinstance"
        self.hosts: RemoteHosts = spicerack.remote().query(query)
        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)
        self._dbctl = spicerack.dbctl()
        self._phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self._mysql = spicerack.mysql()
        self._run_cookbook = spicerack.run_cookbook
        self._do_repool = args.repool
        self.task_id = args.task_id
        self.admin_reason = spicerack.admin_reason(args.reason)
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

    def upgrade_host(self, host):
        """Upgrade mysql version of a single host."""
        host_puppet = self.puppet(host)
        with self.alerting_hosts(host.hosts).downtimed(self.admin_reason, duration=timedelta(hours=24)):
            with host_puppet.disabled(self.admin_reason):
                self._run_upgrade(host)

        # Pool in host after alerting goes green and is enabled, puppet is enabled, replication is in sync
        hostname = str(host).split(".", maxsplit=1)[0]
        reason = f"Upgrade of {host} completed"
        if self._do_repool and self._is_in_dbctl(hostname):
            if self.task_id:
                args = ["--reason", reason, "--task-id", self.task_id, hostname]
            else:
                args = ["--reason", reason, hostname]

            self._run_cookbook("sre.mysql.pool", args, confirm=True)

        if self.task_id:
            self._phab.task_comment(self.task_id, reason)

    def run(self):
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)
        for host in self.hosts.split(1):
            self.upgrade_host(host)

    def _run_upgrade(self, host: RemoteHosts) -> None:
        """Upgrade mysql version of a single host."""
        fqdn = _fqdn(host)
        reason = f"Upgrading {host}"
        if self.task_id:
            self._phab.task_comment(self.task_id, reason)

        hostname = str(host).split(".", maxsplit=1)[0]
        if self._is_in_dbctl(hostname):
            step("depool", f"Depooling {fqdn}")
            hostname = str(host).split(".", maxsplit=1)[0]
            if self.task_id:
                args = ["--reason", reason, "--task-id", self.task_id, hostname]
            else:
                args = ["--reason", reason, hostname]

            self._run_cookbook("sre.mysql.depool", args, confirm=True)

        step("stop_mariadb", f"Stopping mariadb on {fqdn}")
        upgrade_cmd = (
            "DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::='--force-confdef' "
            + "-o Dpkg::Options::='--force-confold' dist-upgrade"
        )
        scripts = [
            # TODO: Migrate to the new MySQL class in spicecrack
            'mysql -e "stop slave;"',
            "systemctl stop mariadb",
            upgrade_cmd,
            "umount /srv",
            "swapoff -a",
        ]
        self._run_scripts(host, scripts)

        reboot_time = datetime.utcnow()
        host.reboot()
        host.wait_reboot_since(reboot_time)

        scripts = [
            'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
            "systemctl start mariadb",
            "mysql_upgrade",
            "systemctl restart mariadb",
            'mysql -e "start slave;"',
        ]
        self._run_scripts(host, scripts)

        reason = f"Upgrade of {host} completed"
        if not self._do_repool:
            self.logger.info("Repooling not requested")
            self._phab.task_comment(self.task_id or "", reason)
            return

        step("catchup_repl_s", f"Catching up replication lag on {fqdn} before removing icinga downtime")
        dbi = get_db_instance(self._mysql, fqdn)
        dbi.wait_for_replication()

        step("wait_icinga_s", f"Waiting for icinga to go green for {fqdn}")
        self.icinga_hosts(host.hosts).wait_for_optimal()

    def _run_scripts(self, host, scripts) -> None:
        for script in scripts:
            try:
                confirm_on_failure(host.run_sync, script)
            except AbortError:
                self.logger.error("%s: execution aborted", script)
                raise
