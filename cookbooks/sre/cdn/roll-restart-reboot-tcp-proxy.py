"""TcpProxy roll operations cookbook."""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class TcpProxyRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of TcpProxy

    Usage example:
        cookbook sre.cdn.roll-restart-reboot-tcp-proxy \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.cdn.roll-restart-reboot-tcp-proxy \
        --reason "HAProxy upgrade" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return TcpProxyRestartRebootRunner(args, self.spicerack)


class TcpProxyRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an TcpProxy cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        aliases = ['A:tcpproxy']
        for dc in ALL_DATACENTERS:
            aliases.append(f'A:tcpproxy-{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:tcpproxy'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['haproxy']
