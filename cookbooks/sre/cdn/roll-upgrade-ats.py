"""Upgrade ATS on CDN nodes

* Depool
* Install specified trafficserver version
* Restart trafficserver
* Repool
"""

from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollUpgradeATS(SREBatchBase):
    r"""Roll upgrade Apache Traffic Server based on parameters.

    Example usage:

        cookbook sre.cdn.roll-upgrade-ats \
            --alias cp-text_codfw \
            --reason '9.2.0 upgrade' \
            --version '9.2.0-1wm1'
    """

    # Reboot/Restart doesn't wipe the cache, so a high grace sleep isn't
    # necessary
    grace_sleep = 300
    batch_max = 1
    valid_actions = ("upgrade",)

    def argument_parser(self):
        """Arguments to add to the standard set"""
        parser = super().argument_parser()
        parser.add_argument('--version', type=str, required=True,
                            help='Specific version to install.')
        return parser

    def get_runner(self, _args):
        """As specified by Spicerack API."""
        return RollUpgradeATSRunner(_args, self.spicerack)


class RollUpgradeATSRunner(SRELBBatchRunnerBase):
    """Upgrade and restart Apache Traffic Server"""

    depool_sleep = 60

    def _upgrade_action(self, hosts: RemoteHosts, reason: Reason) -> None:
        """Install the new ATS version"""
        puppet = self._spicerack.puppet(hosts)
        puppet.run()

        apt_get = self._spicerack.apt_get(hosts)
        confirm_on_failure(apt_get.update)
        confirm_on_failure(apt_get.install, f"trafficserver={self._args.version}")

        self._restart_daemons_action(hosts, reason)

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        aliases = []
        for datacenter in ALL_DATACENTERS:
            aliases.append(f"cp-{datacenter}")
            aliases.append(f"cp-text_{datacenter}")
            aliases.append(f"cp-upload_{datacenter}")
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases

    @property
    def runtime_description(self) -> str:
        """Override the default runtime description"""
        return f"Rolling upgrade/restart of Apache Traffic Server on {self._query()} for {self._args.version}"

    @property
    def restart_daemons(self) -> list:
        """Return a list of daemons to restart when using the restart action"""
        return ["trafficserver"]
