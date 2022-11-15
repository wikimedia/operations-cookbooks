"""Swift proxy roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class SwiftProxiesRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of swift proxies

    Usage example:
        cookbook sre.swift.roll-restart-reboot-proxies \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.swift.roll-restart-reboot-proxies \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return SwiftProxiesRestartRebootRunner(args, self.spicerack)


class SwiftProxiesRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an SwiftProxies cluster"""

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # The following query must include all hosts matching all the allowed_aliases
        return 'A:swift-fe or A:thanos-fe'

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['swift-fe', 'swift-fe-canary', 'swift-fe-codfw', 'swift-fe-eqiad',
                'thanos-fe', 'thanos-fe-codfw', 'thanos-fe-eqiad']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['swift-proxy', 'nginx']
