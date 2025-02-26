"""Stop any jobs running in DC_FROM

Changeprop keeps long running connections with the the mw-jobrunner pods. When we switch
in order to force it to re-resolves the jobrunner service name for its long-running connections,
we need to restart the mw-jobrunner pods.

"""

import logging

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase
from cookbooks.sre.hosts import (
    DEPLOYMENT_HOST,
    DEPLOYMENT_CHARTS_REPO_PATH
)

logger = logging.getLogger(__name__)
HELMFILE_PATH = f"{DEPLOYMENT_CHARTS_REPO_PATH}/helmfile.d/services/mw-jobrunner"
env_vars = ('HELM_CACHE_HOME="/var/cache/helm"',
            'HELM_DATA_HOME="/usr/share/helm"',
            'HELM_HOME="/etc/helm"',
            'HELM_CONFIG_HOME="/etc/helm"')


class RestartJobRunnersRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to restart pods in mw-jobrunner and envoy on jobrunner hosts in DC_FROM."""

    def action(self):
        """Required by base class API."""
        deployment_cname = self.spicerack.dns().resolve_cname(DEPLOYMENT_HOST)
        logger.info('Restarting pods in mw-jobrunner on kubernetes in %s', self.dc_from)
        self.spicerack.remote().query(deployment_cname).run_async(
            f"cd {HELMFILE_PATH}; {' '.join(env_vars)} "
            f"helmfile -e {self.dc_from} --state-values-set roll_restart=1 sync")


class RestartJobRunners(MediaWikiSwitchDCBase):
    """Restart pods in mw-jobrunner on kubernetes and envoy on jobrunner hosts in DC_FROM."""

    runner_class = RestartJobRunnersRunner
