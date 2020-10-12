"""Upgrade/Rollback Hadoop to a newer/previous distribution.

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
import argparse
import logging
import time

from datetime import timedelta

from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.hadoop import (HADOOP_CLUSTER_NAMES, CLUSTER_CUMIN_ALIAS,
                                  MASTER_CUMIN_ALIAS, STANDBY_CUMIN_ALIAS,
                                  WORKERS_CUMIN_ALIAS, HDFS_JOURNAL_CUMIN_ALIAS)


# Some packages that are shipped by the CDH distribution are not available
# for BigTop, so the cookbook needs to workaround this filtering the list
# of packages to install.
CDH_PACKAGES_NOT_IN_BIGTOP = ('avro-libs', 'hadoop-0.20-mapreduce', 'kite',
                              'parquet', 'parquet-format', 'sentry')

__title__ = 'Change Hadoop distribution on a cluster.'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                        choices=HADOOP_CLUSTER_NAMES)
    parser.add_argument('--rollback', action='store_true',
                        help="Set the cookbook to run rollback commands.")

    return parser


def print_run_command_output(run_command_return_status):
    """Helper to print cumin's output"""
    for nodeset, output in run_command_return_status:
        logger.info('\n=========================================\n')
        logger.info('Output for %s', nodeset)
        logger.info(output.message().decode())


def run(args, spicerack):  # pylint: disable=too-many-statements
    """Change Hadoop distribution on a given cluster"""
    if args.cluster == 'test':
        suffix = '-test'
    elif args.cluster == 'analytics':
        suffix = ''
    else:
        raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

    ensure_shell_is_durable()

    spicerack_remote = spicerack.remote()

    hadoop_hosts = spicerack_remote.query(CLUSTER_CUMIN_ALIAS + suffix)
    hadoop_hdfs_journal_workers = spicerack_remote.query(HDFS_JOURNAL_CUMIN_ALIAS + suffix)
    hadoop_workers = spicerack_remote.query(WORKERS_CUMIN_ALIAS + suffix)
    hadoop_master = spicerack_remote.query(MASTER_CUMIN_ALIAS + suffix)
    hadoop_standby = spicerack_remote.query(STANDBY_CUMIN_ALIAS + suffix)

    # TODO: in the future we may want to improve this step, for example
    # verifying automatically that puppet is disabled and/or that all daemons
    # are down.
    ask_confirmation(
        "This cookbook assumes that the stop-cluster.py cookbook ran correctly, "
        "hence that the Hadoop cluster to work on is currently down. It also assumes "
        "that the hosts to be upgraded have puppet disabled (for example, as part of the "
        "aforementioned procedure.")

    yarn_metadata_cleanup_commands = [
        f'setAcl /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot world:anyone:cdrwa',
        f'rmr /yarn-rmstore/analytics{suffix}-hadoop/ZKRMStateRoot']

    ask_confirmation(
        'MANUAL STEP: time to run the Yarn metadata cleanup commands in Zookeeper:\n' +
        str([yarn_metadata_cleanup_commands]))

    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Change Hadoop distribution')

    with icinga.hosts_downtimed(hadoop_hosts.hosts, reason,
                                duration=timedelta(minutes=120)):

        hadoop_workers.run_sync('rm -rf /tmp/hadoop-yarn/*')

        if not args.rollback:
            logger.info(
                'Saving a snapshot of cdh package names and versions in /root/cdh_package_list '
                'on all nodes, and removing all packages.')
            hadoop_hosts.run_sync(
                "dpkg -l | awk '/+cdh/ {print $2}' | tr '\n' ' ' > /root/cdh_package_list")

        logger.info('Removing all packages.')
        hadoop_workers.run_sync("apt-get remove -y `cat /root/cdh_package_list`")
        hadoop_standby.run_sync("apt-get remove -y `cat /root/cdh_package_list`")
        hadoop_master.run_sync("apt-get remove -y `cat /root/cdh_package_list`")

        hadoop_hosts.run_sync('apt-get update')

        print_run_command_output(hadoop_hosts.run_sync('apt-cache policy hadoop | grep Candidate'))
        ask_confirmation('Please verify that the candidate hadoop package is correct.')

        logger.info("Install packages on worker hosts first. Long step.")

        if args.rollback:
            hadoop_workers.run_sync(
                'apt-get install -y `cat /root/cdh_package_list`',
                batch_size=5, batch_sleep=60.0)
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            hadoop_workers.run_sync(
                ("apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                 f"egrep -v '{apt_package_filter}' | tr '\n' ' '`"),
                batch_size=5, batch_sleep=60.0)

        # If the cookbook is running in rollback mode, then there are extra steps to be taken
        # for HDFS Datanodes.
        if args.rollback:
            logger.info('Stop each datanode and start it with the rollback option. Long step.')
            hadoop_workers.run_sync(
                'systemctl stop hadoop-hdfs-datanode',
                'service hadoop-hdfs-datanode rollback',
                batch_size=2, batch_sleep=30.0)

        logger.info('Checking how many java daemons are running on the worker nodes '
                    'after installing the packages.')

        print_run_command_output(hadoop_workers.run_sync(
            'ps aux | grep [j]ava| egrep "JournalNode|DataNode|NodeManager" | grep -v egrep| wc -l'))
        ask_confirmation('Verify that the count is two for non-journal workers, and 3 for journal workers.')

        ask_confirmation(
            'MANUAL STEP: Puppet can be re-enabled manually on Hadoop worker nodes, to check if all configurations '
            'are set up correctly. Continue only once done.')

        logger.info('Install packages on the Hadoop HDFS Master node.')

        if args.rollback:
            hadoop_master.run_sync(
                'apt-get install -y `cat /root/cdh_package_list`')
            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)
            logger.info('Rollback the HDFS Master node state.')
            hadoop_master.run_sync(
                'echo Y | sudo -u hdfs kerberos-run-command hdfs hdfs namenode -rollback')
            # It happened in the past, while testing upgrades, that journal nodes
            # were started in a spurious configuration (no cluster id/version) that
            # prevented the Namenodes to start correctly. The theory is that the rollback
            # command above is the cause, since it does also rollback the state of the JNs.
            # The safest option seems to be to stop journalnodes gracefully,
            # and then start them back again.
            logger.info('Stop/Start of the journalnodes to avoid spurious bugs. Long step.')
            hadoop_hdfs_journal_workers.run_sync(
                'systemctl stop hadoop-hdfs-journalnode',
                batch_size=1, batch_sleep=60.0)
            # We use 'restart' instead of start since sometimes the init.d scripts
            # have trouble in recognizing if a daemon is not running or not when
            # 'start' is requested.
            hadoop_hdfs_journal_workers.run_sync(
                'systemctl restart hadoop-hdfs-journalnode',
                batch_size=1, batch_sleep=60.0)
            logger.info('Starting the HDFS Master node.')
            hadoop_master.run_sync(
                'systemctl start hadoop-hdfs-namenode')
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            hadoop_master.run_async(
                ("apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                 f"egrep -v '{apt_package_filter}' | tr '\n' ' '`"))
            logger.info('Sleeping one minute to let things to stabilize')
            time.sleep(60)
            logger.info('Starting the HDFS Master node with the upgrade option.')
            hadoop_master.run_sync('service hadoop-hdfs-namenode upgrade')

        ask_confirmation(
            'Please check the HDFS Namenode logs on the master node, and continue only when it '
            'seems to be stable. Check also that it transitions to the active state.')

        logger.info('Removing previous Namenode state (if any) from Hadoop HDFS Standby node. '
                    'It does not play well with the next steps (like bootstrapStandby).')
        hadoop_standby.run_sync('rm -rf /var/lib/hadoop/name/previous')

        logger.info('Install packages on the Hadoop HDFS Standby node.')
        if args.rollback:
            hadoop_standby.run_sync(
                'apt-get install -y `cat /root/cdh_package_list`')
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            hadoop_standby.run_sync(
                ("apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                 f"egrep -v '{apt_package_filter}' | tr '\n' ' '`"))

        logger.info('Sleeping one minute to let things to stabilize')
        time.sleep(60)
        logger.info('Formatting the HDFS Standby node and then starting it.')
        hadoop_standby.run_async(
            'echo Y | sudo -u hdfs kerberos-run-command hdfs /usr/bin/hdfs namenode -bootstrapStandby',
            'systemctl start hadoop-hdfs-namenode')

        logger.info('Remember to re-enable puppet on the Hadoop Master/Standby nodes.')
        logger.info('The procedure is completed.')
