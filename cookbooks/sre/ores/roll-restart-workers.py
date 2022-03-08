"""Restart ORES daemons in a cluster"""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable

from cookbooks.sre.ores import ORES_CLUSTERS, ORES_DAEMONS


logger = logging.getLogger(__name__)


class RestartOresWorkers(CookbookBase):
    """Roll restart all ORES daemons in a cluster.

    Every Ores worker host manages multiple daemons:
    * uwsgi
    * celery

    This cookbook, for every ORES node in a cluster:
    - Depool the node
    - Restart uwsgi and celery very gently,
    - Re-pool the node.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(
            description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the ORES cluster to work on.',
                            choices=ORES_CLUSTERS)
        parser.add_argument('--daemons', help='The daemons to restart.', nargs='+',
                            default=ORES_DAEMONS,
                            choices=ORES_DAEMONS)
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestartOresWorkersRunner(args, self.spicerack)


class RestartOresWorkersRunner(CookbookRunnerBase):
    """Restart ORES daemons on a given cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Restart ORES daemons on a given cluster."""
        cluster_cumin_alias = "A:ores-" + args.cluster
        self.cluster = args.cluster
        self.ores_workers = spicerack.remote().query(cluster_cumin_alias)
        self.alerting_hosts = spicerack.alerting_hosts(self.ores_workers.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of ORES\'s daemons.')
        self.daemons = args.daemons
        self.spicerack = spicerack
        self.confctl = spicerack.confctl('node')
        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for ORES {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def run(self):
        """Restart all ORES daemons on a given cluster"""
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=60)):
            logger.info(
                'Restarting daemons (%s), one host at the time.', ','.join(self.daemons))

            lbconfig = self.spicerack.remote().query_confctl(self.confctl, dc=self.cluster,
                                                             cluster="ores", service="ores")
            lbconfig.restart_services(ORES_DAEMONS, ['ores'],
                                      batch_size=1, batch_sleep=120)
        logger.info("All ORES restarts completed!")
