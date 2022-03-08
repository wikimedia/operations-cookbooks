"""Upgrade minor version of MySQL hosts."""

import argparse
import logging
from datetime import datetime, timedelta

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable


class UpgradeMySQL(CookbookBase):
    """Upgrade minor veresion of MySQL hosts.

    Note: It doesn't depool the host (yet).
    """

    def argument_parser(self):
        """CLI parsing, as required by the Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            'query', help='Cumin query to match the host(s) to act upon.'
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UpgradeMySQLRunner(args, self.spicerack)


class UpgradeMySQLRunner(CookbookRunnerBase):
    """Upgrade MySQL cookbook runner."""

    def __init__(self, args, spicerack):
        """Upgrade MySQL on a given set of hosts."""
        ensure_shell_is_durable()

        self.alerting_hosts = spicerack.alerting_hosts
        self.admin_reason = spicerack.admin_reason('MySQL upgrade')
        self.remote = spicerack.remote()
        query = 'P{' + args.query + '} and A:db-all and not A:db-multiinstance'
        self.hosts = spicerack.remote().query(query)
        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)
        if not self.hosts:
            print('No hosts have been found, exiting')
        if len(self.hosts) <= 5:
            self.hosts_message = str(self.hosts)
        else:
            self.hosts_message = f'{len(self.hosts)} hosts'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for {self.hosts_message}'

    def upgrade_host(self, host):
        """Upgrade mysql version of a single host."""
        host_puppet = self.puppet(host)
        with self.alerting_hosts(host.hosts).downtimed(self.admin_reason, duration=timedelta(hours=24)):
            with host_puppet.disabled(self.admin_reason):
                self._run_upgrade(host)

    def run(self):
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)
        for host in self.hosts.split(1):
            self.upgrade_host(host)

    def _run_upgrade(self, host):
        self.logger.info('Stopping mariadb on %s', host)

        upgrade_cmd = "DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::='--force-confdef' " + \
            "-o Dpkg::Options::='--force-confold' dist-upgrade"
        scripts = [
            # TODO: Migrate to the new MySQL class in spicecrack
            'mysql -e "stop slave; SET GLOBAL innodb_buffer_pool_dump_at_shutdown = OFF;"',
            'systemctl stop mariadb',
            upgrade_cmd,
            'umount /srv',
            'swapoff -a',
        ]
        self._run_scripts(host, scripts)

        reboot_time = datetime.utcnow()
        host.reboot()
        host.wait_reboot_since(reboot_time)

        scripts = [
            'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
            'systemctl start mariadb',
            'mysql_upgrade',
            'systemctl restart mariadb',
            'mysql -e "start slave;"',
        ]
        self._run_scripts(host, scripts)

    def _run_scripts(self, host, scripts) -> None:
        for script in scripts:
            try:
                confirm_on_failure(host.run_sync, script)
            except AbortError:
                self.logger.error('%s: execution aborted', script)
                self.results.fail(host.hosts)
                raise
