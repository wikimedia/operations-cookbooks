"""PKI roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SREDiscoveryNoLVSBatchRunnerBase


class PKIRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of PKI

    Usage example:
        cookbook sre.pki.restart-reboot \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.pki.restart-reboot \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return PKIRestartRebootRunner(args, self.spicerack)


class PKIRestartRebootRunner(SREDiscoveryNoLVSBatchRunnerBase):
    """Roll reboot/restart an PKI cluster"""

    service_name = 'pki'

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['pki']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['cfssl-multirootca.service', 'cfssl-ocsprefresh*']
