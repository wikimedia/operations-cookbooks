"""Upgrade HAProxy on CDN nodes"""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class RollUpgradeHAProxy(SREBatchBase):
    r"""Roll upgrade HAProxy based on parameters.

    Example usage:
        cookbook sre.cdn.roll-upgrade-haproxy --alias cp-text_codfw --reason '2.6.9 upgrade' \
            --grace-sleep 30 restart_daemons
        cookbook sre.cdn.roll-upgrade-haproxy --query 'A:cp-eqiad and not P{cp1001*}' --reason '2.6.9 upgrade' \
            --batchsize 2 restart_daemons

    """

    grace_sleep = 60
    batch_max = 2
    valid_actions = ('restart_daemons',)

    # Required
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollUpgradeHAProxyRunner(args, self.spicerack)


class RollUpgradeHAProxyRunner(SREBatchRunnerBase):
    """Upgrade and restart HAProxy"""

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
        return f'rolling upgrade of HAProxy on {self._query()}'

    @property
    def pre_scripts(self):
        """Install the new HAProxy version after depooling the service"""
        return [
            'depool cdn',
            ('DEBIAN_FRONTEND=noninteractive apt-get -q -y '
             '-o DPkg::Options::="--force-confdef" '
             '-o DPkg::Options::="--force-confold" '
             'install haproxy'),
            'run-puppet-agent -q']

    @property
    def post_scripts(self):
        """Repool the services depooled on pre_scripts"""
        return ['pool cdn']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['haproxy']
