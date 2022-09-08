"""Upgrade/Rollback Hadoop to a newer/previous distribution."""
import logging
import time

from datetime import timedelta

from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre.hadoop import (HADOOP_CLUSTER_NAMES, CLUSTER_CUMIN_ALIAS,
                                  MASTER_CUMIN_ALIAS, STANDBY_CUMIN_ALIAS,
                                  WORKERS_CUMIN_ALIAS, HDFS_JOURNAL_CUMIN_ALIAS,
                                  CDH_PACKAGES_NOT_IN_BIGTOP, HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)

logger = logging.getLogger(__name__)


class ChangeHadoopDistro(CookbookBase):
    """Change Hadoop distribution on a cluster.

    This cookbook should be used when there is the need to upgrade/rollback
    one cluster to a specific distribution. A distribution is a collection
    of debian package, as in this case we assume that it also means upgrading
    HDFS from one version to another one.

    The current version of the cookbook is tailored for a Cloudera CDH
    to Apache BigTop upgrade/rollback, but it can surely be made more generic.

    Assumptions:
    - Before running this cookbook, the Hadoop cluster needs to be stopped
      completely (either manually or via cookbook).
    - This cookbook doesn't stop/start puppet, leaving the manual step to
      the operator for safety.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
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
        return ChangeHadoopDistroRunner(args, self.spicerack)


class ChangeHadoopDistroRunner(CookbookRunnerBase):
    """Change Hadoop distribution cookbook runner."""

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

        self.yarn_metadata_cleanup_commands = [
            f'setAcl /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot world:anyone:cdrwa',
            f'rmr /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot']

        self.alerting_hosts = spicerack.alerting_hosts(self.hadoop_hosts.hosts)
        self.admin_reason = spicerack.admin_reason('Change Hadoop distribution')
        self.rollback = args.rollback
        self.cluster = args.cluster

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Hadoop {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def _remove_packages(self):
        """Remove all Hadoop packages on the cluster"""
        logger.info('Removing the Hadoop packages on all nodes.')
        confirm_on_failure(
            self.hadoop_hosts.run_async,
            "apt-get remove -y `cat /root/cdh_package_list`",
            success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)

    def _install_packages_on_workers(self):
        """Install Hadoop packages on Hadoop worker nodes."""
        logger.info("Install packages on worker nodes (long step).")

        if self.rollback:
            confirm_on_failure(
                self.hadoop_workers.run_sync,
                'apt-get install -y `cat /root/cdh_package_list`',
                batch_size=5, batch_sleep=60.0,
                success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            confirm_on_failure(
                self.hadoop_workers.run_sync,
                "apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                f"egrep -v '{apt_package_filter}' | tr '\n' ' '`",
                batch_size=5, batch_sleep=60.0,
                success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)

        # If the cookbook is running in rollback mode, then there are extra steps to be taken
        # for HDFS Datanodes.
        if self.rollback:
            logger.info('Stop each datanode and start it with the rollback option. Long step.')
            confirm_on_failure(
                self.hadoop_workers.run_async,
                'systemctl unmask hadoop-hdfs-datanode', 'service hadoop-hdfs-datanode rollback',
                batch_size=2, batch_sleep=30.0,
                success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)

        logger.info('Checking how many java daemons are running on the worker nodes '
                    'after installing the packages.')

        confirm_on_failure(
            self.hadoop_workers.run_sync,
            'ps aux | egrep "[j]ava.*(JournalNode|DataNode|NodeManager)" | wc -l',
            success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)
        ask_confirmation('Verify that the count is two for non-journal workers, and 3 for journal workers.')

    def _install_packages_on_master(self):
        """Installs the Hadoop packages on the Hadoop Master node."""
        logger.info('Install packages on the Hadoop HDFS Master node.')

        if self.rollback:
            confirm_on_failure(
                self.hadoop_master.run_sync,
                'apt-get install -y `cat /root/cdh_package_list`')
            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)
            logger.info('Rollback the HDFS Master node state.')
            confirm_on_failure(
                self.hadoop_master.run_sync,
                'systemctl unmask hadoop-hdfs-namenode',
                'echo Y | sudo -u hdfs kerberos-run-command hdfs hdfs namenode -rollback')
            # It happened in the past, while testing upgrades, that journal nodes
            # were started in a spurious configuration (no cluster id/version) that
            # prevented the Namenodes to start correctly. The theory is that the rollback
            # command above is the cause, since it does also rollback the state of the JNs.
            # The safest option seems to be to stop journalnodes gracefully,
            # and then start them back again.
            logger.info('Stop/Start of the journalnodes to avoid spurious bugs. Long step.')
            confirm_on_failure(
                self.hadoop_hdfs_journal_workers.run_sync,
                'systemctl stop hadoop-hdfs-journalnode', batch_size=1, batch_sleep=60.0)
            # We use 'restart' instead of start since sometimes the init.d scripts
            # have trouble in recognizing if a daemon is not running or not when
            # 'start' is requested.
            confirm_on_failure(
                self.hadoop_hdfs_journal_workers.run_sync,
                'systemctl restart hadoop-hdfs-journalnode', batch_size=1, batch_sleep=60.0)
            logger.info('Starting the HDFS Master node.')
            confirm_on_failure(
                self.hadoop_master.run_sync,
                'systemctl start hadoop-hdfs-namenode')
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            confirm_on_failure(
                self.hadoop_master.run_async,
                "apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                f"egrep -v '{apt_package_filter}' | tr '\n' ' '`")
            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)
            logger.info('Starting the HDFS Master node with the upgrade option.')
            confirm_on_failure(
                self.hadoop_master.run_sync, 'service hadoop-hdfs-namenode upgrade')

    def _install_packages_on_standby(self):
        """Installs the Hadoop packages on the Hadoop Master standby node."""
        logger.info('Removing previous Namenode state (if any) from Hadoop HDFS Standby node. '
                    'It does not play well with the next steps (like bootstrapStandby).')
        confirm_on_failure(
            self.hadoop_standby.run_sync, 'rm -rf /var/lib/hadoop/name/previous')

        logger.info('Install packages on the Hadoop HDFS Standby node.')
        if self.rollback:
            confirm_on_failure(
                self.hadoop_standby.run_sync,
                'apt-get install -y `cat /root/cdh_package_list`')
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            confirm_on_failure(
                self.hadoop_standby.run_sync,
                "apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                f"egrep -v '{apt_package_filter}' | tr '\n' ' '`")

        logger.info('Sleeping one minute to let things to stabilize')
        time.sleep(60)
        logger.info('Formatting the HDFS Standby node and then starting it.')
        confirm_on_failure(
            self.hadoop_standby.run_async,
            'systemctl unmask hadoop-hdfs-namenode',
            'echo Y | sudo -u hdfs kerberos-run-command hdfs /usr/bin/hdfs namenode -bootstrapStandby',
            'systemctl start hadoop-hdfs-namenode')

    def run(self):
        """Change the Hadoop distribution."""
        # TODO: in the future we may want to improve this step, for example
        # verifying automatically that puppet is disabled and/or that all daemons
        # are down.
        ask_confirmation(
            "This cookbook assumes that the stop-cluster.py cookbook ran correctly, "
            "hence that the Hadoop cluster to work on is currently down. It also assumes "
            "that the hosts to be upgraded have puppet disabled (for example, as part of the "
            "aforementioned procedure.")

        ask_confirmation(
            'MANUAL STEP: time to run the Yarn metadata cleanup commands in Zookeeper:\n' +
            str([self.yarn_metadata_cleanup_commands]))

        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=120)):
            logger.info("Removing the /tmp/hadoop-yarn leftovers on worker nodes.")
            confirm_on_failure(self.hadoop_workers.run_sync, 'rm -rf /tmp/hadoop-yarn/*')

            if not self.rollback:
                logger.info(
                    'Saving a snapshot of cdh package names and versions in /root/cdh_package_list '
                    'on all nodes, and removing all packages.')
                confirm_on_failure(
                    self.hadoop_hosts.run_sync,
                    "dpkg -l | awk '/+cdh/ {print $2}' | tr '\n' ' ' > /root/cdh_package_list")

            if self.rollback:
                # In case of a rollback, the HDFS daemons are masked to prevent any
                # startup caused by a package install. The daemons will need to start
                # with specific rollback options. This does not include Journalnodes
                # (there is a specific roll-restart step later on).
                logger.info('Masking HDFS daemons.')
                confirm_on_failure(
                    self.hadoop_workers.run_sync, "systemctl mask hadoop-hdfs-datanode")
                confirm_on_failure(
                    self.hadoop_standby.run_sync, "systemctl mask hadoop-hdfs-namenode")
                confirm_on_failure(
                    self.hadoop_master.run_sync, "systemctl mask hadoop-hdfs-namenode")

            self._remove_packages()

            confirm_on_failure(
                self.hadoop_hosts.run_async, 'apt-get update',
                batch_size=10, success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)

            confirm_on_failure(
                self.hadoop_hosts.run_sync, 'apt-cache policy hadoop | grep Candidate',
                success_threshold=HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD)
            ask_confirmation('Please verify that the candidate hadoop package is correct across all nodes.')

            self._install_packages_on_workers()

            ask_confirmation(
                'MANUAL STEP: Puppet can be re-enabled manually on Hadoop worker nodes, to check if all configurations '
                'are set up correctly. Continue only once done.')

            self._install_packages_on_master()

            ask_confirmation(
                'Please check the HDFS Namenode logs on the master node, and continue only when it '
                'seems to be stable. Check also that it transitions to the active state.')

            self._install_packages_on_standby()

            logger.info('Remember to re-enable puppet on the Hadoop Master/Standby nodes.')
            logger.info('The procedure is completed.')
