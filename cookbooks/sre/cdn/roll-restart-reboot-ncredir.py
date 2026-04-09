"""NCRedir roll operations cookbook."""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class NCRedirRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of NCRedir

    Usage example:
        cookbook sre.cdn.roll-restart-reboot-ncredir \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.cdn.roll-restart-reboot-ncredir \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return NCRedirRestartRebootRunner(args, self.spicerack)


class NCRedirRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an NCRedir cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        aliases = ['ncredir']
        for dc in ALL_DATACENTERS:
            aliases.append(f'ncredir-{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:ncredir'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['nginx']
