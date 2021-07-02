"""Restart all Hadoop jvm daemons on worker hosts.

The idea for this cookbook is to:
- Set Icinga downtime for all nodes (no puppet disable or depool is needed).
- Roll restart all the Yarn Node Managers in batches of 4/5 hosts.
- Roll restart all the HDFS Journalnodes, one at the time,
  with 30s of delay in between.
- Roll restart all the HDFS Datanodes, a couple of hosts at the time,
  with 30s of delay in between.
- Remove Icinga downtime
"""
import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from . import HADOOP_CLUSTER_NAMES


__title__ = 'Roll restart all the jvm daemons on Hadoop worker nodes'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                        choices=HADOOP_CLUSTER_NAMES)
    parser.add_argument('--yarn-nm-sleep-seconds', type=float, default=30.0,
                        help="Seconds to sleep between each batch of Yarn Nodemanager restarts.")
    parser.add_argument('--hdfs-dn-sleep-seconds', type=float, default=120.0,
                        help="Seconds to sleep between each batch of HDFS Datanode restarts.")
    parser.add_argument('--hdfs-jn-sleep-seconds', type=float, default=120.0,
                        help="Seconds to sleep between each batch of HDFS Journalnode restarts.")
    parser.add_argument('--yarn-nm-batch-size', type=int, default=5,
                        help="Size of each batch of Yarn Nodemanager restarts.")
    parser.add_argument('--hdfs-dn-batch-size', type=int, default=2,
                        help="Size of each batch of HDFS Datanode restarts.")

    return parser


def run(args, spicerack):
    """Restart all Hadoop jvm daemons on a given cluster"""
    if args.cluster == 'test':
        cluster_cumin_alias = 'A:hadoop-worker-test'
        hdfs_jn_cumin_alias = 'A:hadoop-hdfs-journal-test'
    elif args.cluster == 'analytics':
        cluster_cumin_alias = 'A:hadoop-worker'
        hdfs_jn_cumin_alias = 'A:hadoop-hdfs-journal'
    else:
        raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

    ensure_shell_is_durable()

    """Required by Spicerack API."""
    hadoop_workers = spicerack.remote().query(cluster_cumin_alias)
    hadoop_hdfs_journal_workers = spicerack.remote().query(hdfs_jn_cumin_alias)
    icinga_hosts = spicerack.icinga_hosts(hadoop_workers.hosts)
    reason = spicerack.admin_reason('Roll restart of jvm daemons for openjdk upgrade.')

    yarn_nm_batch_size = args.yarn_nm_batch_size
    yarn_nm_sleep = args.yarn_nm_sleep_seconds

    # Not configurable on purpose, too risky!
    hdfs_jn_batch_size = 1
    hdfs_jn_sleep = args.hdfs_jn_sleep_seconds

    hdfs_dn_batch_size = args.hdfs_dn_batch_size
    hdfs_dn_sleep = args.hdfs_dn_sleep_seconds

    # Safety checks
    if hdfs_dn_batch_size > 5:
        ask_confirmation('The HDFS Datanode batch size is bigger than 5, are you sure?')
    if hdfs_dn_sleep < 20:
        ask_confirmation('The HDFS Datanode sleep between each batch is less than 20s, are you sure?')
    if hdfs_jn_sleep < 20:
        ask_confirmation('The HDFS Journalnode sleep between each batch is less than 20s, are you sure?')
    if yarn_nm_batch_size > 10:
        ask_confirmation('The Yarn Nodemanager batch size is bigger than 10, are you sure?')
    if yarn_nm_sleep < 20:
        ask_confirmation('The Yarn Nodemanager sleep between each batch is less than 20s, are you sure?')

    with icinga_hosts.downtimed(reason, duration=timedelta(minutes=120)):
        logger.info("Restarting Yarn Nodemanagers with batch size %s and sleep %s..",
                    yarn_nm_batch_size, yarn_nm_sleep)
        hadoop_workers.run_sync(
            'systemctl restart hadoop-yarn-nodemanager',
            batch_size=yarn_nm_batch_size, batch_sleep=yarn_nm_sleep)

        logger.info("Restarting HDFS Datanodes with batch size %s and sleep %s..",
                    hdfs_dn_batch_size, hdfs_dn_sleep)
        hadoop_workers.run_sync(
            'systemctl restart hadoop-hdfs-datanode',
            batch_size=hdfs_dn_batch_size, batch_sleep=hdfs_dn_sleep)

        logger.info("Restarting HDFS Journalnodes with batch size %s and sleep %s..",
                    hdfs_jn_batch_size, hdfs_jn_sleep)
        hadoop_hdfs_journal_workers.run_sync(
            'systemctl restart hadoop-hdfs-journalnode',
            batch_size=hdfs_jn_batch_size, batch_sleep=hdfs_jn_sleep)

    logger.info("All jvm restarts completed!")
