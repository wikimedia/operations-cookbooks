"""Restart all Presto jvm-based daemons in a cluster"""
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable


logger = logging.getLogger(__name__)


class RestartPrestoWorkers(CookbookBase):
    """Restart all Presto jvm-based daemons in a cluster

    Presto runs only a daemon called 'presto-server' on
    each worker node (including the coordinator).
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('cluster', help='The name of the Presto cluster to work on.',
                            choices=['analytics'])
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestartPrestoWorkersRunner(args, self.spicerack)


class RestartPrestoWorkersRunner(CookbookRunnerBase):
    """Restart Presto cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Restart Presto on a given cluster."""
        ensure_shell_is_durable()
        self.cluster = args.cluster
        self.presto_workers = spicerack.remote().query("A:presto-" + self.cluster)
        self.alerting_hosts = spicerack.alerting_hosts(self.presto_workers.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of all Presto\'s jvm daemons.')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Presto {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def run(self):
        """Restart all Presto jvm daemons on a given cluster"""
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=60)):
            logger.info('Restarting daemons (one host at the time)...')
            commands = ['systemctl restart presto-server']
            self.presto_workers.run_async(*commands, batch_size=1, batch_sleep=120.0)

        logger.info("All Presto jvm restarts completed!")
