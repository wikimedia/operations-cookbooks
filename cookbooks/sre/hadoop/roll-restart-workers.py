"""Restart all Hadoop jvm daemons on worker hosts."""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from . import HADOOP_CLUSTER_NAMES


logger = logging.getLogger(__name__)


class RollRestartWorkers(CookbookBase):
    """Roll restart all the jvm daemons on Hadoop worker nodes.

    - Set Icinga downtime for all nodes (no puppet disable or depool is needed).
    - Roll restart all the Yarn Node Managers in batches of 5 hosts.
    - Roll restart all the HDFS Journalnodes, one at a time,
    with 30s of delay in between.
    - Roll restart all the HDFS Datanodes, a couple of hosts at a time,
    with 30s of delay in between.
    - Remove Icinga downtime.
    - Batch sizes and delay periods are configurable.

    Usage example:
      cookbook sre.hadoop.roll-restart-workers analytics
      cookbook sre.hadoop.roll-restart-workers --yarn-nm-batch-size 2 --hdfs-dn-batch-size 1 test
      cookbook sre.hadoop.roll-restart-workers --yarn-nm-sleep-seconds 60 --hdfs-dn-sleep-seconds 180 backup


    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
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

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartWorkersRunner(args, self.spicerack)


class RollRestartWorkersRunner(CookbookRunnerBase):
    """Hadoop Roll Restart Workers cookbook runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner"""
        if args.cluster == 'test':
            self.cluster_cumin_alias = 'A:hadoop-worker-test'
            self.hdfs_jn_cumin_alias = 'A:hadoop-hdfs-journal-test'
        elif args.cluster == 'analytics':
            self.cluster_cumin_alias = 'A:hadoop-worker'
            self.hdfs_jn_cumin_alias = 'A:hadoop-hdfs-journal'
        else:
            raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

        ensure_shell_is_durable()

        self.cluster = args.cluster
        self.hadoop_workers = spicerack.remote().query(self.cluster_cumin_alias)
        self.hadoop_hdfs_journal_workers = spicerack.remote().query(self.hdfs_jn_cumin_alias)
        self.icinga_hosts = spicerack.icinga_hosts(self.hadoop_workers.hosts)
        self.reason = spicerack.admin_reason('Roll restart of jvm daemons for openjdk upgrade.')

        self.yarn_nm_batch_size = args.yarn_nm_batch_size
        self.yarn_nm_sleep = args.yarn_nm_sleep_seconds

        # Not configurable on purpose, too risky!
        self.hdfs_jn_batch_size = 1
        self.hdfs_jn_sleep = args.hdfs_jn_sleep_seconds

        self.hdfs_dn_batch_size = args.hdfs_dn_batch_size
        self.hdfs_dn_sleep = args.hdfs_dn_sleep_seconds

        # Safety checks
        if self.hdfs_dn_batch_size > 5:
            ask_confirmation('The HDFS Datanode batch size is bigger than 5, are you sure?')
        if self.hdfs_dn_sleep < 20:
            ask_confirmation('The HDFS Datanode sleep between each batch is less than 20s, are you sure?')
        if self.hdfs_jn_sleep < 20:
            ask_confirmation('The HDFS Journalnode sleep between each batch is less than 20s, are you sure?')
        if self.yarn_nm_batch_size > 10:
            ask_confirmation('The Yarn Nodemanager batch size is bigger than 10, are you sure?')
        if self.yarn_nm_sleep < 20:
            ask_confirmation('The Yarn Nodemanager sleep between each batch is less than 20s, are you sure?')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'restart workers for Hadoop {} cluster: {}'.format(self.cluster, self.reason)

    def run(self):
        """Restart all Hadoop jvm daemons on a given cluster"""
        with self.icinga_hosts.downtimed(self.reason, duration=timedelta(minutes=120)):
            logger.info("Restarting Yarn Nodemanagers with batch size %s and sleep %s..",
                        self.yarn_nm_batch_size, self.yarn_nm_sleep)
            self.hadoop_workers.run_sync(
                'systemctl restart hadoop-yarn-nodemanager',
                batch_size=self.yarn_nm_batch_size, batch_sleep=self.yarn_nm_sleep)

            logger.info("Restarting HDFS Datanodes with batch size %s and sleep %s..",
                        self.hdfs_dn_batch_size, self.hdfs_dn_sleep)
            self.hadoop_workers.run_sync(
                'systemctl restart hadoop-hdfs-datanode',
                batch_size=self.hdfs_dn_batch_size, batch_sleep=self.hdfs_dn_sleep)

            logger.info("Restarting HDFS Journalnodes with batch size %s and sleep %s..",
                        self.hdfs_jn_batch_size, self.hdfs_jn_sleep)
            self.hadoop_hdfs_journal_workers.run_sync(
                'systemctl restart hadoop-hdfs-journalnode',
                batch_size=self.hdfs_jn_batch_size, batch_sleep=self.hdfs_jn_sleep)

        logger.info("All jvm restarts completed!")
