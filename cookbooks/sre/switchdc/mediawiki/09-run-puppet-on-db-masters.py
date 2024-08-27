"""Run Puppet on all core DB primaries and remove read-only check downtimes."""

import logging

from cumin import nodeset

from cookbooks.sre.switchdc.mediawiki import READ_ONLY_SERVICE_RE, MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class RunPuppetOnDBPrimariesRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to run puppet on all core DB primaries and remove downtimes."""

    def run(self):
        """Required by Spicerack API."""
        mysql = self.spicerack.mysql_legacy()
        hosts = mysql.get_core_dbs(replication_role="master")
        icinga_hosts = self.spicerack.icinga_hosts(nodeset(str(hosts)))

        logger.info('Running Puppet on all DB masters')
        self.spicerack.remote().query('A:db-role-master').run_sync('run-puppet-agent', batch_size=5)

        logger.info('Rechecking services on Icinga, and waiting for recovery before un-downtiming read-only checks.')
        icinga_hosts.recheck_failed_services()
        icinga_hosts.wait_for_optimal()

        logger.info('Un-downtiming read-only checks.')
        icinga_hosts.remove_service_downtimes(READ_ONLY_SERVICE_RE)


class RunPuppetOnDBPrimaries(MediaWikiSwitchDCBase):
    """Run Puppet on all core DB primaries and remove read-only check downtimes."""

    runner_class = RunPuppetOnDBPrimariesRunner
