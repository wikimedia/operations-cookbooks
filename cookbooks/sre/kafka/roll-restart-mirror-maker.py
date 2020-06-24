"""Restart all Kafka Mirror Maker daemons in a cluster

Kafka Mirror Maker is a simple Consumer group spread on multiple hosts.
Each daemon gets some topic partitions assigned, to then consume all
its messages and produce them to another topic partition (on another cluster).
This is a simple mechanism to replicate Kafka topics across multiple clusters.

Every time a component of a Consumer Group fails its heartbeat with the Kafka
Broker that coordinates the group, a rebalance is issued (to reassign orphaned
partitions to the other members).

"""
import argparse
import logging

from datetime import timedelta

from cookbooks import ArgparseFormatter


__title__ = 'Roll restart all the Kafka Mirror Maker daemons on a cluster'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Kafka Mirror Maker cluster to work on.',
                        choices=['main-eqiad', 'jumbo', 'main-codfw'])
    parser.add_argument('--batch-sleep-seconds', type=float, default=120.0,
                        help="Seconds to sleep between each restart.")
    return parser


def run(args, spicerack):
    """Restart all Kafka Mirror Maker daemons on a given cluster"""
    cluster_cumin_alias = "A:kafka-mirror-maker-" + args.cluster

    """Required by Spicerack API."""
    kafka_mirror_makers = spicerack.remote().query(cluster_cumin_alias)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of jvm daemons.')

    with icinga.hosts_downtimed(kafka_mirror_makers.hosts, reason,
                                duration=timedelta(minutes=120)):

        kafka_mirror_makers.run_sync(
            'systemctl restart kafka-mirror.service', batch_size=1, batch_sleep=args.batch_sleep_seconds)

    logger.info('All Kafka Mirror Maker restarts completed!')
