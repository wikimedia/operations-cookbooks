"""Cookbook to roll-restart a Restbase cluster."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RestbaseRestart(SREBatchBase):
    """Cookbook to perform a rolling restart of Restbase

    Usage example:
        cookbook sre.misc-clusters.roll-restart-restbase \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    owner_team = 'Data Persistence'
    batch_default = 1
    valid_actions = ('restart_daemons',)
    grace_sleep = 5

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestbaseRestartRunner(args, self.spicerack)


class RestbaseRestartRunner(SRELBBatchRunnerBase):
    """Roll restart an Restbase cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['restbase', 'restbase-canary', 'restbase-codfw', 'restbase-eqiad']

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:restbase'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['restbase']
