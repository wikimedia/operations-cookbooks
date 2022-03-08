"""Restart all Hadoop jvm daemons on master hosts."""

import argparse
import logging
import time

from datetime import timedelta

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from . import HADOOP_CLUSTER_NAMES

logger = logging.getLogger(__name__)


class RollRestartMasters(CookbookBase):
    """Restart all the jvm daemons on Hadoop master nodes.

    - Set Icinga/Alertmanager downtime for all nodes (no puppet disable or depool is needed).
    - Check the status of Yarn Resourcemanager daemons (expecting an active and a standby node).
    - Check the status of HDFS Namenode daemons (expecting an active and a standby node).
    - Restart one Resource Manager at a time.
    - Force a failover of HDFS Namenode to the standby node.
    - Restart one HDFS Namenode (the current standby).
    - Force a failover of HDFS Namenode to the standby node (basically restoring the prev. state).
    - Restart the Mapreduce history server (only on one host).
    - Remove the Icinga/Alertmanager downtime.

    Usage example:
      cookbook sre.hadoop.roll-restart-masters analytics
      cookbook sre.hadoop.roll-restart-masters --yarn-rm-sleep-seconds 180 --hdfs-nn-sleep-seconds 900 backup
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                            choices=HADOOP_CLUSTER_NAMES)
        parser.add_argument('--yarn-rm-sleep-seconds', type=float, default=60.0,
                            help="Seconds to sleep between Yarn Resourcemanager restarts.")
        parser.add_argument('--hdfs-nn-sleep-seconds', type=float, default=600.0,
                            help="Seconds to sleep between HDFS Namenode restarts.")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartMastersRunner(args, self.spicerack)


class RollRestartMastersRunner(CookbookRunnerBase):
    """Hadoop Roll Restart Masters cookbook runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        if args.cluster == 'test':
            self.suffix = '-test'
            self.cluster = 'test'
        elif args.cluster == 'analytics':
            self.suffix = ''
            self.cluster = 'analytics'
        else:
            raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

        ensure_shell_is_durable()

        self.remote = spicerack.remote()
        self.hadoop_master = self.remote.query('A:hadoop-master' + self.suffix)
        self.hadoop_standby = self.remote.query('A:hadoop-standby' + self.suffix)
        self.alerting_hosts = spicerack.alerting_hosts(self.hadoop_master.hosts | self.hadoop_standby.hosts)
        self.admin_reason = spicerack.admin_reason('Restart of jvm daemons.')

        self.yarn_rm_sleep = args.yarn_rm_sleep_seconds
        self.hdfs_nn_sleep = args.hdfs_nn_sleep_seconds

        # Safety checks
        if self.hdfs_nn_sleep < 600:
            ask_confirmation('The HDFS Namenode restart sleep is less than 600s, are you sure?')
        if self.yarn_rm_sleep < 60:
            ask_confirmation('The Yarn Resourcemanager restart sleep is less than 60s, are you sure?')
        if len(self.hadoop_master) != 1:
            raise RuntimeError("Expecting exactly one Hadoop master server. Found: {}".format(self.hadoop_master))
        if len(self.hadoop_standby) != 1:
            raise RuntimeError("Expecting exactly one Hadoop standby server. Found: {}".format(self.hadoop_standby))

        # This is needed due to the format of the hostname in the command, for example:
        # sudo -u hdfs /usr/bin/hdfs haadmin -getServiceState an-master1001-eqiad-wmnet
        self.hadoop_master_service = self.hadoop_master.hosts[0].replace('.', '-')
        self.hadoop_standby_service = self.hadoop_standby.hosts[0].replace('.', '-')

        logger.info('Checking HDFS and Yarn daemon status. We expect active statuses on the Master node, '
                    'and standby statuses on the other. Please do not proceed otherwise.')

        print_hadoop_service_state(
            self.hadoop_master, self.hadoop_master_service, self.hadoop_standby_service)

        ask_confirmation('Please make sure that the active/standby nodes shown are correct.')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'restart masters for Hadoop {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def run(self):
        """Restart all Hadoop jvm daemons on a given cluster"""
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=120)):
            logger.info("Restarting Yarn Resourcemanager on Master.")
            self.hadoop_master.run_sync('systemctl restart hadoop-yarn-resourcemanager')
            logger.info("Sleeping %s seconds.", self.yarn_rm_sleep)
            time.sleep(self.yarn_rm_sleep)
            logger.info("Restarting Yarn Resourcemanager on Standby.")
            self.hadoop_standby.run_sync('systemctl restart hadoop-yarn-resourcemanager')

            print_hadoop_service_state(
                self.hadoop_master, self.hadoop_master_service, self.hadoop_standby_service, hdfs=False)

            ask_confirmation("Ok to proceed with HDFS Namenodes ?")

            logger.info("Run manual HDFS failover from master to standby.")
            run_hdfs_namenode_failover(self.hadoop_master, self.hadoop_master_service, self.hadoop_standby_service)

            logger.info("Sleeping 30 seconds.")
            time.sleep(30)

            logger.info("Restart HDFS Namenode on the master.")
            self.hadoop_master.run_async(
                'systemctl restart hadoop-hdfs-zkfc',
                'systemctl restart hadoop-hdfs-namenode')

            logger.info("Sleeping %s seconds.", self.hdfs_nn_sleep)
            time.sleep(self.hdfs_nn_sleep)

            print_hadoop_service_state(
                self.hadoop_master, self.hadoop_master_service, self.hadoop_standby_service, yarn=False)

            ask_confirmation("Ok to proceed?")

            logger.info("Run manual HDFS failover from standby to master.")
            run_hdfs_namenode_failover(self.hadoop_master, self.hadoop_standby_service, self.hadoop_master_service)

            logger.info("Sleeping 30 seconds.")
            time.sleep(30)

            logger.info("Restart HDFS Namenode on the standby.")
            self.hadoop_standby.run_async(
                'systemctl restart hadoop-hdfs-zkfc',
                'systemctl restart hadoop-hdfs-namenode')

            logger.info("Sleeping %s seconds.", self.hdfs_nn_sleep)
            time.sleep(self.hdfs_nn_sleep)

            logger.info("\n\nSummary of active/standby statuses after the restarts:")

            print_hadoop_service_state(
                self.hadoop_master, self.hadoop_master_service, self.hadoop_standby_service)

            logger.info("Restart MapReduce historyserver on the master.")
            self.hadoop_master.run_sync('systemctl restart hadoop-mapreduce-historyserver')

    logger.info("All jvm restarts completed!")


def print_hadoop_service_state(
        remote_handle, hadoop_master_service_name, hadoop_standby_service_name,
        yarn=True, hdfs=True):
    """Helper to print the status of Hadoop daemons"""
    logger.info("Checking Master/Standby status.")
    if hdfs:
        logger.info("\nMaster status for HDFS:")
        remote_handle.run_sync(
            'kerberos-run-command hdfs hdfs haadmin -getServiceState ' +
            hadoop_master_service_name)

    if yarn:
        logger.info("\nMaster status for Yarn:")
        remote_handle.run_sync(
            'kerberos-run-command yarn yarn rmadmin -getServiceState ' +
            hadoop_master_service_name)

    if hdfs:
        logger.info("\nStandby status for HDFS:")
        remote_handle.run_sync(
            'kerberos-run-command hdfs hdfs haadmin -getServiceState ' +
            hadoop_standby_service_name)

    if yarn:
        logger.info("\nStandby status for Yarn:")
        remote_handle.run_sync(
            'kerberos-run-command yarn yarn rmadmin -getServiceState ' +
            hadoop_standby_service_name)


def run_hdfs_namenode_failover(remote_handle, active_hadoop_service, standby_hadoop_service):
    """Helper to execute a HDFS Namenode failover."""
    logger.info(
        "Run manual HDFS Namenode failover from %s to %s.",
        active_hadoop_service, standby_hadoop_service)
    remote_handle.run_sync(
        "kerberos-run-command hdfs hdfs haadmin -failover {} {}"
        .format(active_hadoop_service, standby_hadoop_service))
