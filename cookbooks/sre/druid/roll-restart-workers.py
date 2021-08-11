"""Restart all Druid jvm-base daemons in a cluster"""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.druid import DRUID_DAEMONS


logger = logging.getLogger(__name__)


class RestartDruidWorkers(CookbookBase):
    """Roll restart all Druid jvm daemons in a cluster.

    Every Druid worker host manages multiple daemons:
    * Historical
    * Broker
    * MiddleManager
    * Overlord
    * Coordinator

    All of them are heavily relying on Zookeeper, that is currently
    co-located in the same cluster (but not part of this cookbook).

    Upstream suggests to restart one daemon at the time when restarting
    or upgrading, the order is not extremely important. The longest and
    more delicate restart is the Historical's, since the daemon needs
    to load Druid segments from cache/HDFS and it might take minutes to
    complete. Up to now we rely on tailing logs on the host to establish
    when all the segments are loaded (and make sure that everything looks
    ok), but in the future we'll probably rely on a metric to have a more
    automated cookbook.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(
            description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Druid cluster to work on.',
                            choices=['public', 'analytics', 'test'])
        parser.add_argument('--daemons', help='The daemons to restart.', nargs='+',
                            default=DRUID_DAEMONS,
                            choices=DRUID_DAEMONS)
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestartDruidWorkersRunner(args, self.spicerack)


class RestartDruidWorkersRunner(CookbookRunnerBase):
    """Restart druid daemons on a given cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Restart druid daemons on a given cluster."""
        cluster_cumin_alias = "A:druid-" + args.cluster
        self.need_depool = False
        if args.cluster == 'public':
            self.need_depool = True
        self.cluster = args.cluster
        self.druid_workers = spicerack.remote().query(cluster_cumin_alias)
        self.icinga_hosts = spicerack.icinga_hosts(self.druid_workers.hosts)
        self.reason = spicerack.admin_reason('Roll restart of Druid jvm daemons.')
        self.daemons = args.daemons
        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Druid {} cluster: {}'.format(self.cluster, self.reason)

    def run(self):
        """Restart all Druid jvm daemons on a given cluster"""
        with self.icinga_hosts.downtimed(self.reason, duration=timedelta(minutes=60)):
            logger.info(
                'Restarting daemons (%s), one host at the time.', ','.join(self.daemons))
            commands = []
            for daemon in self.daemons:
                commands.append('systemctl restart druid-' + daemon)
                if daemon == 'overlord':
                    commands.append('sleep 300')
                else:
                    commands.append('sleep 30')

            if self.need_depool:
                commands = ['depool', 'sleep 60'] + commands + ['pool']

            self.druid_workers.run_async(
                *commands, batch_size=1, batch_sleep=120.0)

        logger.info("All Druid jvm restarts completed!")
