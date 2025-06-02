"""Rolling restart of Apache Traffic Server on CDN nodes"""

from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollRestartATS(SREBatchBase):
    r"""Rolling restart Apache Traffic Server in the CDN cluster.

    This is only for the restart of ATS. For the ATS upgrade, see:
        sre.cdn.roll-upgrade-ats

    * Depool (service `cdn')
    * Restart ATS (trafficserver.service)
    * Repool (service `cdn')

    Example usage:

        cookbook sre.cdn.roll-restart-ats \
            --alias cp-text_codfw \
            --reason 'systemd unit change'
    """

    # Restart doesn't wipe the cache, so the below intervals are fine.
    min_grace_sleep = 30
    grace_sleep = 60
    # Batch of 1 is more than enough.
    batch_max = 1

    valid_actions = ('restart_daemons',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartATSRunner(args, self.spicerack)


class RollRestartATSRunner(SRELBBatchRunnerBase):
    """Rolling restart of Apache Traffic Server"""

    depool_sleep = 10
    repool_sleep = 10

    @property
    def allowed_aliases(self) -> list:
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
    def restart_daemons(self) -> list:
        """Return a list of daemons to restart when using the restart action"""
        return ['trafficserver']

    @property
    def depool_services(self):
        """Property to return a list of specific services to depool/repool. If empty means all services."""
        return ['cdn']
