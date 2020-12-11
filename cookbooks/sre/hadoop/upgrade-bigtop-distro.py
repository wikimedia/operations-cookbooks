"""Upgrade/Rollback Hadoop to a newer/previous Apache Bigtop distribution."""

import argparse
import logging
import time

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.hadoop import (HADOOP_CLUSTER_NAMES, CLUSTER_CUMIN_ALIAS,
                                  MASTER_CUMIN_ALIAS, STANDBY_CUMIN_ALIAS,
                                  WORKERS_CUMIN_ALIAS, HDFS_JOURNAL_CUMIN_ALIAS,
                                  BIGTOP_WORKER_PACKAGES, BIGTOP_MASTER_PACKAGES,
                                  BIGTOP_MASTER_STANDBY_PACKAGES)

logger = logging.getLogger(__name__)


class UpgradeBigtop(CookbookBase):
    """Upgrade the Apache Bigtop Hadoop distribution on a cluster.

    This cookbook should be used when there is the need to upgrade/rollback
    one cluster from/to a specific Apache Bigtop distribution.
    A distribution is a collection of debian packages, as in this case
    we assume that it also means upgrading HDFS from one version to another one.

    Assumptions:
    - The Hadoop cluster already runs a version of Bigtop.
    - Before running this cookbook, the Hadoop cluster needs to be stopped
      completely (either manually or via cookbook).
    - This cookbook doesn't stop/start puppet, leaving the manual step to
      the operator for safety.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                            choices=HADOOP_CLUSTER_NAMES)
        parser.add_argument('--workers-cumin-query', required=False, help='A cumin query string to select '
                            'the Hadoop workers to work on. This limits/overrides the selection of the '
                            'cluster argument. It should be used to resume a rollback/upgrade that '
                            'failed on a limited number of hosts.')
        parser.add_argument('--journalnodes-cumin-query', required=False, help='A cumin query string to select '
                            'the Hadoop Journal nodes to work on. This limits/overrides the selection of the '
                            'cluster argument. It should be used to resume a rollback/upgrade that '
                            'failed on a limited number of hosts.')
        parser.add_argument('--rollback', action='store_true',
                            help="Set the cookbook to run rollback commands.")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UpgradeBigtopRunner(args, self.spicerack)


class UpgradeBigtopRunner(CookbookRunnerBase):
    """Upgrade Bigtop cookbook runner"""

    def __init__(self, args, spicerack):
        """Change Hadoop distribution on a given cluster"""
        if args.cluster == 'test':
            suffix = '-test'
        elif args.cluster == 'analytics':
            suffix = ''
        else:
            raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

        ensure_shell_is_durable()

        spicerack_remote = spicerack.remote()

        self.hadoop_hosts = spicerack_remote.query(CLUSTER_CUMIN_ALIAS + suffix)
        self.hadoop_hdfs_journal_workers = spicerack_remote.query(HDFS_JOURNAL_CUMIN_ALIAS + suffix)
        if args.journalnodes_cumin_query:
            hadoop_hdfs_journal_override = spicerack_remote.query(args.journalnodes_cumin_query)
            self.hadoop_hdfs_journal_workers = spicerack_remote.query(
                "D{{{}}}".format(
                    self.hadoop_hdfs_journal_workers.hosts.intersection(hadoop_hdfs_journal_override.hosts)))
            ask_confirmation(
                'The cookbook will run only on the following journal hosts ({}), please verify that '
                'the list looks correct: {}'
                .format(len(self.hadoop_hdfs_journal_workers), self.hadoop_hdfs_journal_workers))

        self.hadoop_workers = spicerack_remote.query(WORKERS_CUMIN_ALIAS + suffix)
        if args.workers_cumin_query:
            hadoop_workers_override = spicerack_remote.query(args.workers_cumin_query)
            self.hadoop_workers = spicerack_remote.query(
                "D{{{}}}".format(self.hadoop_workers.hosts.intersection(hadoop_workers_override.hosts)))
            ask_confirmation(
                'The cookbook will run only on the following worker hosts ({}), please verify that '
                'the list looks correct: {}'
                .format(len(self.hadoop_workers), self.hadoop_workers))

        self.hadoop_master = spicerack_remote.query(MASTER_CUMIN_ALIAS + suffix)
        self.hadoop_standby = spicerack_remote.query(STANDBY_CUMIN_ALIAS + suffix)

        self.icinga = spicerack.icinga()
        self.reason = spicerack.admin_reason('Change Hadoop distribution')

        self.rollback = args.rollback
        self.cluster = args.cluster

        self.apt_install_options = '-y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"'

        # Workaround needed for https://issues.apache.org/jira/browse/YARN-8310
        self.yarn_metadata_cleanup_commands = [
            f'setAcl /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot world:anyone:cdrwa',
            f'rmr /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot']

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Hadoop {} cluster: {}'.format(self.cluster, self.reason)

    def run(self):
        """Upgrade or Rollback the Hadoop distribution"""
        ask_confirmation(
            "This cookbook assumes that the stop-cluster.py cookbook ran correctly, "
            "hence that the Hadoop cluster to work on is currently down. It also assumes "
            "that the hosts to be upgraded have puppet disabled (for example, as part of the "
            "aforementioned procedure.")

        ask_confirmation(
            'MANUAL STEP: time to run the Yarn metadata cleanup commands in Zookeeper:\n {}' +
            str([self.yarn_metadata_cleanup_commands]))

        with self.icinga.hosts_downtimed(self.hadoop_hosts.hosts, self.reason,
                                         duration=timedelta(minutes=120)):

            self.hadoop_workers.run_sync('rm -rf /tmp/hadoop-yarn/*')

            if self.rollback:
                logger.info('Removing all packages.')
                self.hadoop_workers.run_sync(
                    "apt-get remove -y {}".format(' '.join(BIGTOP_WORKER_PACKAGES)))
                self.hadoop_standby.run_sync(
                    "apt-get remove -y {}".format(' '.join(BIGTOP_MASTER_STANDBY_PACKAGES)))
                self.hadoop_master.run_sync(
                    "apt-get remove -y {}".format(' '.join(BIGTOP_MASTER_PACKAGES)))

            self.hadoop_hosts.run_sync('apt-get update')

            self.hadoop_hosts.run_sync('apt-cache policy hadoop | grep Candidate')
            ask_confirmation('Please verify that the candidate hadoop package is correct.')

            logger.info("Install packages on worker hosts first. Long step.")

            self.hadoop_workers.run_sync(
                "apt-get install {} {}".format(self.apt_install_options, ' '.join(BIGTOP_WORKER_PACKAGES)),
                batch_size=5, batch_sleep=60.0)

            # If the cookbook is running in rollback mode, then there are extra steps to be taken
            # for HDFS Datanodes.
            if self.rollback:
                logger.info('Stop each datanode and start it with the rollback option. Long step.')
                self.hadoop_workers.run_async(
                    'systemctl stop hadoop-hdfs-datanode',
                    'service hadoop-hdfs-datanode rollback',
                    batch_size=2, batch_sleep=30.0)

            logger.info('Checking how many java daemons are running on the worker nodes '
                        'after installing the packages.')

            self.hadoop_workers.run_sync(
                'ps aux | egrep "[j]ava.*(JournalNode|DataNode|NodeManager)" | wc -l')
            ask_confirmation('Verify that the count is two for non-journal workers, and 3 for journal workers.')

            ask_confirmation(
                'MANUAL STEP: Puppet can be re-enabled manually on Hadoop worker nodes, to check if all configurations '
                'are set up correctly. Continue only once done.')

            logger.info('Install packages on the Hadoop HDFS Master node.')

            self.hadoop_master.run_sync(
                "apt-get install {} {}".format(
                    self.apt_install_options,
                    ' '.join(BIGTOP_MASTER_PACKAGES)))
            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)

            if self.rollback:
                logger.info('Rollback the HDFS Master node state.')
                self.hadoop_master.run_sync(
                    'echo Y | sudo -u hdfs kerberos-run-command hdfs hdfs namenode -rollback')
                # It happened in the past, while testing upgrades, that journal nodes
                # were started in a spurious configuration (no cluster id/version) that
                # prevented the Namenodes to start correctly. The theory is that the rollback
                # command above is the cause, since it does also rollback the state of the JNs.
                # The safest option seems to be to stop journalnodes gracefully,
                # and then start them back again.
                logger.info('Stop/Start of the journalnodes to avoid spurious bugs. Long step.')
                self.hadoop_hdfs_journal_workers.run_sync(
                    'systemctl stop hadoop-hdfs-journalnode',
                    batch_size=1, batch_sleep=60.0)
                # We use 'restart' instead of start since sometimes the init.d scripts
                # have trouble in recognizing if a daemon is not running or not when
                # 'start' is requested.
                self.hadoop_hdfs_journal_workers.run_sync(
                    'systemctl restart hadoop-hdfs-journalnode',
                    batch_size=1, batch_sleep=60.0)
                logger.info('Starting the HDFS Master node.')
                self.hadoop_master.run_sync(
                    'systemctl start hadoop-hdfs-namenode')
            else:
                logger.info('Starting the HDFS Master node with the upgrade option.')
                self.hadoop_master.run_sync('service hadoop-hdfs-namenode upgrade')

            ask_confirmation(
                'Please check the HDFS Namenode logs on the master node, and continue only when it '
                'seems to be stable. Check also that it transitions to the active state.')

            logger.info('Removing previous Namenode state (if any) from Hadoop HDFS Standby node. '
                        'It does not play well with the next steps (like bootstrapStandby).')
            self.hadoop_standby.run_sync('rm -rfv /var/lib/hadoop/name/previous')

            logger.info('Install packages on the Hadoop HDFS Standby node.')
            self.hadoop_standby.run_sync(
                "apt-get install {} {}".format(
                    self.apt_install_options,
                    ' '.join(BIGTOP_MASTER_STANDBY_PACKAGES)))

            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)
            logger.info('Formatting the HDFS Standby node and then starting it.')
            self.hadoop_standby.run_async(
                'systemctl stop hadoop-hdfs-namenode',
                'echo Y | sudo -u hdfs kerberos-run-command hdfs /usr/bin/hdfs namenode -bootstrapStandby',
                'systemctl start hadoop-hdfs-namenode')

            logger.info('Remember to re-enable puppet on the Hadoop Master/Standby nodes.')
            logger.info('The procedure is completed.')
