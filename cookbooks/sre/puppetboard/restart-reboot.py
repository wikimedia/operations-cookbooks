"""Cookbook to perform a rolling reboot of Puppetboard replicas"""
from cookbooks.sre import SREBatchBase, SREDiscoveryNoLVSBatchRunnerBase


class PuppetboardRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot of Puppetboard replicas

    Usage example:
        cookbook sre.puppetboard.restart-reboot \
        --reason "Rolling reboot to pick up new kernel" reboot

    """

    batch_default = 1

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return PuppetboardRestartRebootRunner(args, self.spicerack)


class PuppetboardRestartRebootRunner(SREDiscoveryNoLVSBatchRunnerBase):
    """Roll reboot/restart an Puppetboard replica cluster"""

    service_name = 'puppetboard'

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['puppetboard']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['uwsgi-puppetboard', 'envoyproxy', 'apache2']
