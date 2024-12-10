"""Cookbook to perform a rolling reboot of config-master hosts"""
from cookbooks.sre import SREBatchBase, SREDiscoveryNoLVSBatchRunnerBase


class ConfigMasterRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot of config-master nodes

    Usage example:
        cookbook sre.discovery.restart-reboot-config-master \
        --reason "Rolling reboot to pick up new kernel" reboot \
        --alias config-master

    """

    owner_team = 'Infrastructure Foundations'
    batch_default = 1

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ConfigMasterRestartRebootRunner(args, self.spicerack)


class ConfigMasterRestartRebootRunner(SREDiscoveryNoLVSBatchRunnerBase):
    """Roll reboot/restart an ConfigMaster replica cluster"""

    # Name of discovery record
    service_name = 'config-master'

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['config-master', 'config-master-eqiad', 'config-master-codfw']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['envoyproxy', 'apache2']
