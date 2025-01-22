"""Roll restart tcp-mss-clamper"""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollRestartTCPMSSClamper(SREBatchBase):
    r"""Roll restart tcp-mss-clamper based on parameters.

    Example usage:
        cookbook sre.cdn.roll-restart-tcp-mss-clamper --alias cp-text_codfw --reason 'config update' \
            --grace-sleep 30 restart_daemons

    """

    grace_sleep = 60
    valid_actions = ('restart_daemons',)
    batch_max = 2

    # Required
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartTCPMSSClamperRunner(args, self.spicerack)


class RollRestartTCPMSSClamperRunner(SRELBBatchRunnerBase):
    """tcp-mss-clamper reboot class"""

    disable_puppet_on_restart = True
    depool_threshold = 2  # Maximum allowed batch size
    depool_sleep = 30  # Seconds to sleep after the depool before the restart
    repool_sleep = 30  # Seconds to sleep before the repool after the restart

    @property
    def allowed_aliases(self):
        """Required by RebootRunnerBase"""
        aliases = ['cp']
        for role in ('text', 'upload'):
            aliases.append(f'cp-{role}')
            for dc in ALL_DATACENTERS:
                aliases.append(f'cp-{dc}')
                aliases.append(f'cp-{role}_{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['tcp-mss-clamper']

    @property
    def depool_services(self):
        """Property to return a list of specific services to depool/repool. If empty means all services."""
        return ['cdn']
