"""Restart all Presto jvm-based daemons in a cluster

Presto runs only a daemon called 'presto-server' on
each worker node (including the coordinator).

"""
import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = 'Roll restart all the jvm daemons on Presto worker nodes'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Presto cluster to work on.',
                        choices=['analytics'])
    return parser


def run(args, spicerack):
    """Restart all Presto jvm daemons on a given cluster"""
    cluster_cumin_alias = "A:presto-" + args.cluster

    ensure_shell_is_durable()
    presto_workers = spicerack.remote().query(cluster_cumin_alias)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of all Presto\'s jvm daemons.')

    with icinga.hosts_downtimed(presto_workers.hosts, reason,
                                duration=timedelta(minutes=60)):

        logger.info('Restarting daemons (one host at the time)...')
        commands = ['systemctl restart presto-server']
        presto_workers.run_async(*commands, batch_size=1, batch_sleep=120.0)

    logger.info("All Presto jvm restarts completed!")
