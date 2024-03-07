"""Stop any jobs running in DC_FROM

Changeprop keeps long running connections with the the mw-jobrunner pods. When we switch
in order to force it to re-resolves the jobrunner service name for its long-running connections,
we need to restart the mw-jobrunner pods. We additionally restart the envoyproxy service on
the remaining baremetal jobrunners.

TODO: In the next switchcover (Sept 2024), the envoy restart should be removed as it's will not
      be relevant anymore.
"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args
from cookbooks.sre.hosts import (
    DEPLOYMENT_HOST,
    DEPLOYMENT_CHARTS_REPO_PATH
)


__title__ = 'Restart pods in mw-jobrunner on kubernetes in DC_FROM.'
logger = logging.getLogger(__name__)
HELMFILE_PATH = f"{DEPLOYMENT_CHARTS_REPO_PATH}/helmfile.d/services/mw-jobrunner"
env_vars = ('HELM_CACHE_HOME="/var/cache/helm"',
            'HELM_DATA_HOME="/usr/share/helm"',
            'HELM_HOME="/etc/helm"',
            'HELM_CONFIG_HOME="/etc/helm"')


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    deployment_cname = spicerack.dns().resolve_cname(DEPLOYMENT_HOST)
    logger.info('Restarting envoy on jobrunners in %s', args.dc_from)
    spicerack.remote().query(f"A:mw-jobrunner-{args.dc_from}").run_sync('systemctl restart envoyproxy')
    logger.info('Restarting pods in mw-jobrunner on kubernetes in %s', args.dc_from)
    spicerack.remote().query(deployment_cname).run_async(
        f"cd {HELMFILE_PATH}; {' '.join(env_vars)} helmfile -e {args.dc_from} --state-values-set roll_restart=1 sync")
