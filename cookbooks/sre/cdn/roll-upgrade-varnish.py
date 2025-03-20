"""Upgrade Varnish on CDN nodes."""

from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollUpgradeVarnish(SREBatchBase):
    r"""Roll upgrade Varnish in the CDN cluster.

    * Depool
    * Install Varnish and components, letting apt dictate the versions
    * Restart related services
    * Repool

    Example usage:

        cookbook sre.cdn.roll-upgrade-varnish \
            --alias cp-text_codfw \
            --reason '7.1 upgrade'
    """

    # Reboot/Restart wipes the in-memory cache, so a high grace sleep is
    # required.
    batch_max = 1
    grace_sleep = 1500
    min_grace_sleep = 1200
    valid_actions = ('upgrade',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollUpgradeVarnishRunner(args, self.spicerack)


class RollUpgradeVarnishRunner(SRELBBatchRunnerBase):
    """Upgrade and restart Varnish"""

    depool_sleep = 60

    def _upgrade_action(self, hosts: RemoteHosts, reason: Reason) -> None:
        """Install packages"""
        puppet = self._spicerack.puppet(hosts)
        puppet.run()

        apt_get = self._spicerack.apt_get(hosts)
        confirm_on_failure(apt_get.update)
        confirm_on_failure(
            apt_get.install,
            "libvarnishapi3",
            "libvmod-netmapper",
            "libvmod-querysort",
            "varnish",
            "varnish-modules",
            "varnish-re2",
        )

        self._restart_daemons_action(hosts, reason)
        # Run any potential corrective measures.
        puppet.run()

    @property
    def allowed_aliases(self):
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
    def runtime_description(self):
        """Override the default runtime description"""
        return f'rolling upgrade of Varnish on {self._query()}'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return [
            "prometheus-varnish-exporter@frontend",
            "varnish-frontend",
            # varnishkafka is linked against libvarnishapi
            "varnishkafka-all",
            "varnishmtail@default",
            "varnishmtail@internal",
        ]
