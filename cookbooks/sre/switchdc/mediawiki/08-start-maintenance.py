"""Start MediaWiki maintenance jobs."""

import logging

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase
from cookbooks.sre.hosts import (
    DEPLOYMENT_HOST,
    DEPLOYMENT_CHARTS_REPO_PATH
)

logger = logging.getLogger(__name__)
HELMFILE_SERVICES = f"{DEPLOYMENT_CHARTS_REPO_PATH}/helmfile.d/services/"
env_vars = ('HELM_CACHE_HOME="/var/cache/helm"',
            'HELM_DATA_HOME="/usr/share/helm"',
            'HELM_HOME="/etc/helm"',
            'HELM_CONFIG_HOME="/etc/helm"')


class StartMaintenanceJobsRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to start MediaWiki maintenance jobs."""

    def action(self):
        """Required by base class API."""
        logger.info('Starting MediaWiki maintenance jobs in %s', self.dc_to)

        # We have changed the primary_dc key value by now, so we can
        # safely recreate jobs in k8s. We should also clean up
        # existing resoures from the old jobs that might not be
        # destroyed along with the various cron jobs.

        # Remove resources for mw-cron that aren't needed in the from_dc
        self.reapply_k8s("mw-cron", self.dc_from)
        # Create resources and restart crons in the to_dc
        self.reapply_k8s("mw-cron", self.dc_to)

    def reapply_k8s(self, service, dc):
        """Run apply for a given k8s service."""
        deployment_cname = self.spicerack.dns().resolve_cname(DEPLOYMENT_HOST)
        logger.info('Creating jobs for %s on kubernetes in %s', service, dc)
        self.spicerack.remote().query(deployment_cname).run_async(
            f"cd {HELMFILE_SERVICES}{service}; {' '.join(env_vars)} "
            f"helmfile -e {dc} apply")


class StartMaintenanceJobs(MediaWikiSwitchDCBase):
    """Start MediaWiki maintenance jobs."""

    runner_class = StartMaintenanceJobsRunner
