"""Upgrade HAProxy on CDN nodes"""

from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollUpgradeHAProxy(SREBatchBase):
    r"""Roll upgrade HAProxy in the CDN cluster.

    * Depool
    * Install HAProxy, letting apt dictate the versions
    * Restart related services
    * Repool

    Example usage:

        cookbook sre.cdn.roll-upgrade-haproxy \
            --alias cp-text_codfw \
            --reason '2.6.9 upgrade' \
            --grace-sleep 30 \
            --batchsize 2

        cookbook sre.cdn.roll-upgrade-haproxy \
            --query 'A:cp-eqiad and not P{cp1001*}' \
            --reason '2.6.9 upgrade'
    """

    min_grace_sleep = 30
    grace_sleep = 60
    batch_max = 2
    valid_actions = ('upgrade',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollUpgradeHAProxyRunner(args, self.spicerack)


class RollUpgradeHAProxyRunner(SRELBBatchRunnerBase):
    """Upgrade and restart HAProxy"""

    depool_sleep = 60

    def _upgrade_action(self, hosts: RemoteHosts, reason: Reason) -> None:
        """Install packages, letting apt dictate the versions to use."""
        puppet = self._spicerack.puppet(hosts)
        # Recently-merged puppet changed might need to still be synced.
        puppet.run()

        apt_get = self._spicerack.apt_get(hosts)
        confirm_on_failure(apt_get.update)
        confirm_on_failure(apt_get.install, 'haproxy')

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
        return f'rolling upgrade of HAProxy on {self._query()} - {self._reason}'

    @property
    def restart_daemons(self) -> list:
        """Return a list of daemons to restart when using the restart action"""
        return ['haproxy']
