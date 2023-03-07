"""Maps rolling restart cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class MapsRestart(SREBatchBase):
    """Cookbook to perform a rolling restart of maps services

    Usage example:
        cookbook sre.maps.roll-restart \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2
    valid_actions = ('restart_daemons',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return MapsRestartRunner(args, self.spicerack)


class MapsRestartRunner(SRELBBatchRunnerBase):
    """Roll restart an maps cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['maps-replica', 'maps-replica-codfw', 'maps-replica-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['tileratorui', 'kartotherian', 'nginx', 'postgresql']
