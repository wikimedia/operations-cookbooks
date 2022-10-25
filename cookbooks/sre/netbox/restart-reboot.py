"""Cookbook to perform a rolling reboot of Netbox replicas"""
from cookbooks.sre import SREBatchBase, SREDiscoveryNoLVSBatchRunnerBase


class NetboxRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot of Netbox replicas

    Usage example:
        cookbook sre.netbox.restart-reboot \
        --reason "Rolling reboot to pick up new kernel" reboot

    """

    batch_default = 1

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return NetboxRestartRebootRunner(args, self.spicerack)


class NetboxRestartRebootRunner(SREDiscoveryNoLVSBatchRunnerBase):
    """Roll reboot/restart an Netbox replica cluster"""

    service_name = 'netbox'

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['netbox']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['uwsgi-netbox', ',uwsgi-netbox-scriptproxy', 'redis-server', 'apache2']
