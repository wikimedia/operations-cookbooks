"""Maps master rolling restart/reboot cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class MapsMasterRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling restart of maps masters

    Usage example:
        cookbook sre.maps.roll-restart-reboot-master \
          --reason "Rolling restart to pick new OpenSSL" restart_daemons

        cookbook sre.maps.roll-restart-reboot-master \
          --reason "Rolling reboot to pick up new kernel" reboot
    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return MapsMasterRestartRebootRunner(args, self.spicerack)


class MapsMasterRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll restart an maps master"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['maps-master', 'maps-master-codfw', 'maps-master-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['imposm', 'kartotherian', 'nginx', 'postgresql']
