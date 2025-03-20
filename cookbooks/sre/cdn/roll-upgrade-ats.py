"""Upgrade Apache Traffic Server on CDN nodes"""

from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollUpgradeATS(SREBatchBase):
    r"""Roll upgrade Apache Traffic Server in the CDN cluster.

    * Depool
    * Install ATS and components, letting apt dictate the versions
    * Restart related services
    * Repool

    Example usage:

        cookbook sre.cdn.roll-upgrade-ats \
            --alias cp-text_codfw \
            --reason '9.2.0 upgrade'
    """

    # Reboot/Restart doesn't wipe the cache, so a high grace sleep isn't
    # necessary
    min_grace_sleep = 30
    grace_sleep = 60
    batch_max = 1
    valid_actions = ("upgrade",)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollUpgradeATSRunner(args, self.spicerack)


class RollUpgradeATSRunner(SRELBBatchRunnerBase):
    """Upgrade and restart Apache Traffic Server"""

    depool_sleep = 60

    def _upgrade_action(self, hosts: RemoteHosts, reason: Reason) -> None:
        """Install packages"""
        puppet = self._spicerack.puppet(hosts)
        # Recently-merged puppet changed might need to still be synced.
        puppet.run()

        apt_get = self._spicerack.apt_get(hosts)
        confirm_on_failure(apt_get.update)
        confirm_on_failure(apt_get.install,
                           'trafficserver',
                           'trafficserver-experimental-plugins')

        self._restart_daemons_action(hosts, reason)
        # Run any potential corrective measures.
        puppet.run()

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        aliases = ['cp']
        for role in ('text', 'upload'):
            aliases.append(f'cp-{role}')
            for dc in ALL_DATACENTERS:
                aliases.append(f'cp-{dc}')
                aliases.append(f'cp-{role}_{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases

    @property
    def runtime_description(self) -> str:
        """Override the default runtime description"""
        return f'Rolling upgrade of Apache Traffic Server on {self._query()} - {self._reason}'

    @property
    def restart_daemons(self) -> list:
        """Return a list of daemons to restart when using the restart action"""
        return ["trafficserver"]
