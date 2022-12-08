"""Roll restart Varnish frontend based on parameters"""
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class RollRestartVarnish(SREBatchBase):
    r"""Roll restart Varnish frontend based on parameters.

    Example usage:
        cookbook sre.cdn.roll-restart-varnish --alias cp-text_codfw --reason 'Emergency restart' \
            --grace-sleep 30 restart_daemons
        cookbook sre.cdn.roll-restart-varnish --query 'A:cp-eqiad and not P{cp1001*}' --reason 'Emergency restart' \
            --batchsize 2 restart_daemons --threads-limited 100000

    """

    grace_sleep = 60
    valid_actions = ('restart_daemons',)

    def argument_parser(self):
        """Argument parser"""
        parser = super().argument_parser()
        parser.add_argument(
            '--threads-limited',
            type=int,
            help=('Restart Varnish only if the varnish_main_threads_limited metric variation in the last 10 minutes '
                  'metric is above the given threshold.'))
        return parser

    # Required
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartVarnishRunner(args, self.spicerack)


class RollRestartVarnishRunner(SRELBBatchRunnerBase):
    """An example reboot class"""

    disable_puppet_on_restart = True
    depool_threshold = 2  # Maximum allowed batch size
    depool_sleep = 20  # Seconds to sleep after the depool before the restart
    repool_sleep = 15  # Seconds to sleep before the repool after the restart

    def _query(self) -> str:
        """Return the formatted query filtered by the threads_limited parameter."""
        query = super()._query()
        limit = self._args.threads_limited
        if limit is None:
            return query

        thanos = self._spicerack.thanos()
        metrics = thanos.query(
            f'irate(varnish_main_threads_limited{{prometheus="ops",layer="frontend"}}[10m]) > {limit}')

        threshold_hosts = [metric['metric']['instance'].split(':')[0] for metric in metrics]
        self.logger.info('Found %d hosts with varnish_main_threads_limited over the threshold of %d',
                         len(threshold_hosts), limit)
        metric_query = ','.join(f'{host}*' for host in threshold_hosts)
        if not metric_query:
            raise RuntimeError('No matching varnish host has the irate at 10 minutes of varnish_main_threads_limited '
                               f'over the threshold of {limit}')

        return f'{query} and P{{{metric_query}}}'

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
        query = self._args.query if self._args.query else f'A:{self._args.alias}'
        threads = ''
        if self._args.threads_limited is not None:
            threads = f' with threads_limited > {self._args.threads_limited}'

        return f'rolling restart of Varnish on {len(self.all_hosts)} hosts{threads} matching query {query}'

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['varnish-frontend']

    @property
    def depool_services(self):
        """Property to return a list of specific services to depool/repool. If empty means all services."""
        return ['cdn']
