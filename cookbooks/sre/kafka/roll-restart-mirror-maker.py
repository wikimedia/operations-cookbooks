"""Restart all Kafka Mirror Maker daemons in a cluster"""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from cookbooks import ArgparseFormatter

logger = logging.getLogger(__name__)


class RollRestartMirrorMaker(CookbookBase):
    """Restart all Kafka Mirror Maker daemons on a given cluster

    Kafka Mirror Maker is a simple Consumer group spread on multiple hosts.
    Each daemon gets some topic partitions assigned, to then consume all
    its messages and produce them to another topic partition (on another cluster).
    This is a simple mechanism to replicate Kafka topics across multiple clusters.

    Every time a component of a Consumer Group fails its heartbeat with the Kafka
    Broker that coordinates the group, a rebalance is issued (to reassign orphaned
    partitions to the other members).

    We colocate Kafka MirrorMaker processes on the target Kafka cluster brokers.
    I.e. MirrorMaker that handles main-eqiad -> jumbo-eqiad mirroring
    lives on all jumbo-eqiad brokers.

    NOTE: If we ever mirror from multiple cluster into one, that target cluster may
    have multiple 'cluster instances' of MirrorMaker running on the same host,
    e.g. logging-eqiad -> jumbo-eqiad + main-eqiad -> jumbo-eqiad.  As of
    2021-06, this is not the case anywhere, but it may change, so be aware.
    (We should probably move MirrorMaker into k8s anyway).

    Usage example:
        cookbook sre.kafka.roll-mirror-maker jumbo-eqiad
        cookbook sre.kafka.roll-mirror-maker --batch-sleep-seconds 180 main-eqiad

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Kafka Mirror Maker cluster to work on.',
                            choices=['main-eqiad', 'jumbo-eqiad', 'main-codfw', 'test-eqiad'])
        parser.add_argument('--batch-sleep-seconds', type=float, default=120.0,
                            help="Seconds to sleep between each restart.")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartMirrorMakerRunner(args, self.spicerack)


class RollRestartMirrorMakerRunner(CookbookRunnerBase):
    """Kafka Mirror Maker roll restart runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.cluster_cumin_alias = "A:kafka-mirror-maker-" + args.cluster
        self.kafka_mirror_makers = spicerack.remote().query(self.cluster_cumin_alias)
        self.icinga_hosts = spicerack.icinga_hosts(self.kafka_mirror_makers.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of jvm daemons.')
        self.batch_sleep_seconds = args.batch_sleep_seconds

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'restart MirrorMaker for Kafka {} cluster: {}'.format(
            self.cluster_cumin_alias, self.admin_reason.reason)

    def run(self):
        """Restart all Kafka Mirror Maker daemons on a given cluster"""
        with self.icinga_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=120)):

            self.kafka_mirror_makers.run_sync(
                'systemctl restart kafka-mirror.service', batch_size=1, batch_sleep=self.batch_sleep_seconds)

        logger.info('All Kafka Mirror Maker restarts completed!')
