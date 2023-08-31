"""Clone a MySQL database in another host."""
import logging
from datetime import timedelta
import re
import time

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.config import load_yaml_config
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable
import transferpy.transfer
from transferpy.Transferer import Transferer


class CloneMySQL(CookbookBase):
    """Clone one MySQL host into another.

    Note: It doesn't depool the host (yet).
    """

    def argument_parser(self):
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            '--source', help='Cumin query to match the host of the source.'
        )
        parser.add_argument(
            '--target', help='Cumin query to match the host of the target.'
        )
        parser.add_argument(
            '--primary', help='Cumin query to match the host of the primary.'
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return CloneMySQLRunner(args, self.spicerack)


class CloneMySQLRunner(CookbookRunnerBase):
    """Clone MySQL cookbook runner."""

    def __init__(self, args, spicerack):
        """Clone one MySQL host into another."""
        ensure_shell_is_durable()

        self.alerting_hosts = spicerack.alerting_hosts
        self.admin_reason = spicerack.admin_reason('MySQL Clone')
        self.remote = spicerack.remote()
        source = 'P{' + args.source + '} and A:db-all and not A:db-multiinstance'
        source_hosts = spicerack.remote().query(source)
        if len(source_hosts) != 1:
            print('No suitable host as source have been found, exiting')
            raise RuntimeError
        self.source_host = list(source_hosts.split(1))[0]
        target = 'P{' + args.target + '} and A:db-all and not A:db-multiinstance'
        target_hosts = spicerack.remote().query(target)
        if len(target_hosts) != 1:
            print('No suitable host as target have been found, exiting')
            raise RuntimeError
        self.target_host = list(target_hosts.split(1))[0]
        primary = 'P{' + args.primary + '} and A:db-all and not A:db-multiinstance'
        primary_hosts = spicerack.remote().query(primary)
        if len(primary_hosts) != 1:
            print('No suitable host as the primary have been found, exiting')
            raise RuntimeError
        self.primary_host = list(primary_hosts.split(1))[0]
        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)
        # Other prep
        self.tp_options = dict(transferpy.transfer.parse_configurations(
            transferpy.transfer.CONFIG_FILE))
        # this also handles string->bool conversion where necessary
        self.tp_options = transferpy.transfer.assign_default_options(
            self.tp_options)
        # If source and target are in different dcs, encrypt
        netbox_source = spicerack.netbox_server(str(self.source_host).split('.', maxsplit=1)[0])
        netbox_target = spicerack.netbox_server(str(self.target_host).split('.', maxsplit=1)[0])
        if netbox_source.as_dict()['site']['slug'] != netbox_target.as_dict()['site']['slug']:
            self.tp_options['encrypt'] = True
        config = load_yaml_config(spicerack.config_dir / "mysql" / "config.yaml")
        self.replication_user = config['replication_user']
        self.replication_password = config['replication_password']

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'of {self.source_host} onto {self.target_host}'

    def run(self):
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)
        hosts_to_downtime = [list(self.source_host.hosts)[0], list(self.target_host.hosts)[0]]
        self.alerting_hosts(hosts_to_downtime).downtime(self.admin_reason, duration=timedelta(hours=48))
        self._run_clone()

    def _run_clone(self):
        self.logger.info('Stopping mariadb on %s', self.source_host)

        self._run_scripts(self.source_host, ['mysql -e "STOP SLAVE;"'])
        replication_status = self.source_host.run_sync('mysql -e "SHOW SLAVE STATUS\\G"')
        replication_status = list(replication_status)[0][1].message().decode('utf-8')
        binlog_file = re.findall(r'\sMaster_Log_File:\s*(\S+)', replication_status)
        repl_position = re.findall(r'\sExec_Master_Log_Pos:\s*(\d+)', replication_status)
        if len(binlog_file) != 1 or len(repl_position) != 1:
            self.logger.error('Cloud not find the replication position aborting')
            raise AbortError
        binlog_file = binlog_file[0]
        repl_position = repl_position[0]
        self._run_scripts(self.source_host, ['service mariadb stop'])

        self._run_scripts(self.target_host, ['mysql -e "STOP SLAVE;"', 'service mariadb stop', 'rm -rf /srv/sqldata/'])

        t = Transferer(str(self.source_host), '/srv/sqldata', [str(self.target_host)], ['/srv/'], self.tp_options)
        # transfer.py produces a lot of log chatter, cf T330882
        self.logger.debug("Starting transferpy, expect cumin errors")
        r = t.run()
        self.logger.debug("Transferpy complete")
        if r[0] != 0:
            raise RuntimeError("Transfer failed")

        scripts = [
            'chown -R mysql. /srv/*',
            'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
            'systemctl start mariadb',
            'mysql -e "STOP SLAVE; RESET SLAVE ALL"',
        ]
        self._run_scripts(self.target_host, scripts)

        sql = (
            f"CHANGE MASTER TO master_host='{self.primary_host}', "
            f"master_port=3306, master_ssl=1, master_log_file='{binlog_file}', "
            f"master_log_pos={repl_position}, master_user='{self.replication_user}', "
            f"master_password='{self.replication_password}';"
        )
        sql = sql.replace('"', '\\"')
        scripts = [
            f'mysql -e "{sql}"',
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.target_host, scripts)

        self._wait_for_replication(self.target_host)

        scripts = [
            'mysql -e "STOP SLAVE;"',
            'mysql -e "CHANGE MASTER TO MASTER_USE_GTID=Slave_pos;"',
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.target_host, scripts)

        scripts = [
            'systemctl start mariadb',
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.source_host, scripts)

    def _run_scripts(self, host, scripts) -> None:
        for script in scripts:
            try:
                confirm_on_failure(host.run_sync, script)
            except AbortError:
                self.logger.error('%s: execution aborted', script)
                raise

    def _wait_for_replication(self, host) -> None:
        replag = 1000.0
        while replag > 1.0:
            replag = self._get_replication(host)
            if ((replag is None) or (replag > 1.0)):
                print('Waiting for replag to catch up')
                time.sleep(60)

    def _get_replication(self, host) -> float:
        query = """
        SELECT greatest(0, TIMESTAMPDIFF(MICROSECOND, max(ts), UTC_TIMESTAMP(6)) - 500000)/1000000
        FROM heartbeat.heartbeat
        ORDER BY ts LIMIT 1;
        """.replace('\n', '')
        query_res = host.run_sync(f'mysql -e "{query}"')
        query_res = list(query_res)[0][1].message().decode('utf-8')
        replag = 1000.0
        for line in query_res.split('\n'):
            if not line.strip():
                continue
            count = line.strip()
            try:
                count = float(count)
            except ValueError:
                continue
            replag = count
        return replag
