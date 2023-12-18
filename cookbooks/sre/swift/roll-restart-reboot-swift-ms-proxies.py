"""Swift proxy roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class SwiftProxiesMSRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of mediastorage swift proxies

    Usage example:
        cookbook sre.swift.roll-restart-reboot-swift-ms-proxies \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.swift.roll-restart-reboot-swift-ms-proxies \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return SwiftProxiesMSRestartRebootRunner(args, self.spicerack)


class SwiftProxiesMSRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart a media storage Swift proxy cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['swift-fe', 'swift-fe-canary', 'swift-fe-codfw', 'swift-fe-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['swift-proxy', 'envoyproxy']
