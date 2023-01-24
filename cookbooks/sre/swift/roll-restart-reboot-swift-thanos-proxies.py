"""Swift proxy roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class SwiftProxiesThanosRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of Thanos swift proxies

    Usage example:
        cookbook sre.swift.roll-restart-reboot-swift-thanos-proxies \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.swift.roll-restart-reboot-swift-thanos-proxies \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return SwiftProxiesThanosRestartRebootRunner(args, self.spicerack)


class SwiftProxiesThanosRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an SwiftProxiesThanos cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['thanos-fe', 'thanos-fe-codfw', 'thanos-fe-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['swift-proxy', 'envoyproxy']
