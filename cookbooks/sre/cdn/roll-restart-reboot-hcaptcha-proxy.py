"""hcaptcha-proxy roll operations cookbook."""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class HcaptchaProxyRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of hcaptcha-proxy

    Usage example:
        cookbook sre.cdn.roll-restart-reboot-hcaptcha-proxy \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.cdn.roll-restart-reboot-hcaptcha-proxy \
            --reason "nginx upgrade" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return HcaptchaProxyRestartRebootRunner(args, self.spicerack)


class HcaptchaProxyRestartRebootRunner(SREBatchRunnerBase):
    """Roll reboot/restart an hcaptcha-proxy cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        aliases = ['A:hcaptcha-proxy']
        for dc in ALL_DATACENTERS:
            aliases.append(f'A:hcaptcha-proxy-{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:hcaptcha-proxy'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['nginx', 'bird']
