"""Maps rolling restart/reboot cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class MapsRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling restart of maps services

    Usage example:
        cookbook sre.maps.roll-restart-reboot \
          --reason "Rolling restart to pick new OpenSSL" restart_daemons

        cookbook sre.maps.roll-restart-reboot \
          --reason "Rolling reboot to pick up new kernel" reboot
    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return MapsRestartRebootRunner(args, self.spicerack)


class MapsRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll restart an maps cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['maps-replica', 'maps-replica-codfw', 'maps-replica-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['kartotherian', 'nginx', 'postgresql']
