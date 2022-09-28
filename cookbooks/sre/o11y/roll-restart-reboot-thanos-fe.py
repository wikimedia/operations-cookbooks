"""Thanos frontend roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class ThanosFrontendRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of Thanos frontends

    Usage example:
        cookbook sre.o11y.roll-restart-reboot-thanos-fe \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.o11y.roll-restart-reboot-thanos-fe \
           --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ThanosFrontendRestartRebootRunner(args, self.spicerack)


class ThanosFrontendRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart a Thanos frontend cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['thanos-fe', 'thanos-fe-codfw', 'thanos-fe-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['swift-proxy', 'thanos-bucket-web', 'thanos-query-frontend',
                'thanos-query', 'thanos-store']
