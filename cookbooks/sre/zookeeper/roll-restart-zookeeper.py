"""Restart all Zookeeper daemons in a cluster

Zoookeeper can run stand-alone or in a cluster for distributed coordination.
It is used by a lot of Apache projects like Kafka, Hadoop, Druid, etc..

There is always one master in a cluster, the other daemons are acting as
followers (ready to take the leadership role if needed).

The idea of this cookbook is to carefully check the status of all daemons
in a cluster before restarting each of them.

"""
import argparse
import logging

from datetime import timedelta
from spicerack.interactive import ask_confirmation, ensure_shell_is_durable


__title__ = 'Roll restart all the Zookeeper daemons on a cluster'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('cluster', help='The name of the Zookeeper cluster to work on.',
                        choices=['main-eqiad', 'main-codfw', 'druid-public', 'druid-analytics'])
    parser.add_argument('--batch-sleep-seconds', type=float, default=120.0,
                        help="Seconds to sleep between each restart.")
    return parser


def run(args, spicerack):
    """Restart all Zookeeper daemons on a given cluster"""
    cluster_cumin_alias = "A:zookeeper-" + args.cluster

    ensure_shell_is_durable()

    zookeeper = spicerack.remote().query(cluster_cumin_alias)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of jvm daemons.')

    zookeeper_status = zookeeper.run_sync('echo stats | nc -q 1 localhost 2181')
    for nodeset, output in zookeeper_status:
        logger.info('Output for %s', nodeset)
        logger.info(output.message().decode())

    ask_confirmation(
        'Please check the status of Zookeeper before proceeding.'
        'There must be only one leader and the rest must be followers.')

    with icinga.hosts_downtimed(zookeeper.hosts, reason,
                                duration=timedelta(minutes=120)):

        zookeeper.run_sync(
          'systemctl restart zookeeper', batch_size=1,
          batch_sleep=args.batch_sleep_seconds)

    logger.info('All Zookeeper restarts completed!')
