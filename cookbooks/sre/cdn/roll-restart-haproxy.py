"""Rolling restart of haproxy on CDN nodes"""

from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollRestartHaproxy(SREBatchBase):
    r"""Rolling restart haproxy in the CDN cluster.

    This is only for the restart of haproxy. For the haproxy upgrade, see:
        sre.cdn.roll-upgrade-haproxy

    * Depool (service `cdn')
    * Restart haproxy (haproxy.service)
    * Repool (service `cdn')

    Example usage:

        cookbook sre.cdn.roll-restart-haproxy \
            --alias cp-text_codfw \
            --reason 'OpenSSL update'
    """

    min_grace_sleep = 30
    grace_sleep = 60
    batch_max = 2

    valid_actions = ('restart_daemons',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartHaproxyRunner(args, self.spicerack)


class RollRestartHaproxyRunner(SRELBBatchRunnerBase):
    """Rolling restart of Apache Traffic Server"""

    depool_sleep = 30
    repool_sleep = 30

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
        return f'rolling restart of HAProxy on {self._query()} - {self._args.reason} ({self._args.task_id})'

    @property
    def restart_daemons(self) -> list:
        """Return a list of daemons to restart when using the restart action"""
        return ['haproxy']

    @property
    def depool_services(self):
        """Property to return a list of specific services to depool/repool. If empty means all services."""
        return ['cdn']
