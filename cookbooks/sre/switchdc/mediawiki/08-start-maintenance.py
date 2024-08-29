"""Start MediaWiki maintenance jobs."""

import logging

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class StartMaintenanceJobsRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to start MediaWiki maintenance jobs."""

    def action(self):
        """Required by base class API."""
        logger.info('Starting MediaWiki maintenance jobs in %s', self.dc_to)

        mw_maintenance = self.spicerack.remote().query('A:mw-maintenance')
        mw_maintenance.run_sync(f'run-puppet-agent --enable "{self.reason}"')

        mediawiki = self.spicerack.mediawiki()
        # Verify timers are enabled in both DCs
        mediawiki.check_periodic_jobs_enabled(self.dc_to)
        mediawiki.check_periodic_jobs_enabled(self.dc_from)


class StartMaintenanceJobs(MediaWikiSwitchDCBase):
    """Start MediaWiki maintenance jobs."""

    runner_class = StartMaintenanceJobsRunner
