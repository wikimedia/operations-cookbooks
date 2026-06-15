"""Roll restart purged frontend based on parameters"""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollRestartPurged(SREBatchBase):
    r"""Roll restart purged based on parameters.

    Example usage:
        cookbook sre.cdn.roll-restart-purged \
            --alias cp-text_codfw \
            --reason 'Emergency restart' \
            --grace-sleep 30 \
            restart_daemons

        cookbook sre.cdn.roll-restart-purged \
            --query 'A:cp-eqiad and not P{cp1001*}' \
            --reason 'Emergency restart' \
            --batchsize 2 \
            restart_daemons
    """

    batch_default = 2
    grace_sleep = 120

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartPurgedRunner(args, self.spicerack)


class RollRestartPurgedRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart a purged cluster"""

    depool_threshold = 6  # Maximum allowed batch size
    depool_sleep = 15  # Seconds to sleep after the depool before the restart
    repool_sleep = 60  # Seconds to sleep before the repool after the restart

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
        return ['purged']
