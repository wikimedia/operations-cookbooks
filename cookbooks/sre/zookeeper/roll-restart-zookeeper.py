"""Restart all Zookeeper daemons in a cluster"""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable


logger = logging.getLogger(__name__)


class RollRestartZookeeper(CookbookBase):
    """Restart all Zookeeper daemons in a cluster

    Zoookeeper can run stand-alone or in a cluster for distributed coordination.
    It is used by a lot of Apache projects like Kafka, Hadoop, Druid, etc..

    There is always one master in a cluster, the other daemons are acting as
    followers (ready to take the leadership role if needed).

    The idea of this cookbook is to carefully check the status of all daemons
    in a cluster before restarting each of them.

    Usage example:
        cookbook sre.zookeeper.roll-restart-zookeeper analytics
        cookbook sre.zookeeper.roll-restart-zookeeper --batch-sleep-seconds 180 main-eqiad

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Zookeeper cluster to work on.',
                            choices=['main-eqiad', 'main-codfw', 'druid-public',
                                     'druid-analytics', 'analytics'])
        parser.add_argument('--batch-sleep-seconds', type=float, default=120.0,
                            help="Seconds to sleep between each restart.")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartZookeeperRunner(args, self.spicerack)


class RollRestartZookeeperRunner(CookbookRunnerBase):
    """Zookeeper Roll Restart cookbook runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner"""
        ensure_shell_is_durable()

        self.cluster_cumin_alias = "A:zookeeper-" + args.cluster
        self.zookeeper = spicerack.remote().query(self.cluster_cumin_alias)
        self.alerting_hosts = spicerack.alerting_hosts(self.zookeeper.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of jvm daemons.')
        self.batch_sleep_seconds = args.batch_sleep_seconds

        # Safety checks
        self.zookeeper.run_sync('echo stats | nc -q 1 localhost 2181')

        logger.info('\n=========================================\n')
        ask_confirmation(
            'Please check the status of Zookeeper before proceeding.'
            'There must be only one leader and the rest must be followers.')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Zookeeper {} cluster: {}'.format(
            self.cluster_cumin_alias, self.admin_reason.reason)

    def run(self):
        """Restart all Zookeeper daemons on a given cluster"""
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=120)):
            self.zookeeper.run_sync('systemctl restart zookeeper', batch_size=1, batch_sleep=self.batch_sleep_seconds)

        logger.info('All Zookeeper restarts completed!')
