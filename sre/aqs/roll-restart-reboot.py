"""AQS roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class AQSRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of AQS

    Usage example:
        cookbook sre.aqs.roll-restart-reboot \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.aqs.roll-restart-reboot \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return AQSRestartRebootRunner(args, self.spicerack)


class AQSRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an AQS cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['aqs', 'aqs-eqiad', 'aqs-codfw', 'aqs-canary']

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:aqs'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['aqs']
