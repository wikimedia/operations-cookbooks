"""Stop an Hadoop cluster.

This cookbook should be used when some important maintenance is
needed to be performed, like upgrading the package distribution.
The cookbook takes care of gracefully shutdown the hadoop cluster.

"""
import argparse
import logging
import time

from datetime import timedelta

from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from . import (HADOOP_CLUSTER_NAMES, CLUSTER_CUMIN_ALIAS,
               MASTER_CUMIN_ALIAS, STANDBY_CUMIN_ALIAS,
               WORKERS_CUMIN_ALIAS, HDFS_JOURNAL_CUMIN_ALIAS)


__title__ = 'Gracefully stop an Hadoop cluster.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                        choices=HADOOP_CLUSTER_NAMES)

    return parser


def print_run_command_output(run_command_return_status):
    """Helper to print cumin's output"""
    for nodeset, output in run_command_return_status:
        logger.info('\n=========================================\n')
        logger.info('Output for %s', nodeset)
        logger.info(output.message().decode())


def safety_checks(hadoop_workers, hadoop_master, hadoop_standby):
    """Run safety checks before starting any invasive action in the cluster."""
    ask_confirmation(
        'This cookbook takes care only about the Hadoop master/standby/worker nodes, '
        'so please make sure that coordinator, presto, druid, notebooks, clients, etc.. '
        'are all downtimed and with puppet disabled.\n'
        'Important things to check/remember:\n'
        '- systemd timers running jobs need to be stopped.'
        '- oozie/hive/presto need to be shutdown on the coordinator node.'
        '- /mnt/hdfs mountpoints need to be unmounted from clients.'
        '- Hue/Jupyter processes should be down.'
        '- The Cluster needs to be drained from running jobs.\n\n'
        'Also please note that the Hadoop cluster\'s hosts will be left with puppet '
        'disabled, to prevent daemons to be restarted before time.')

    logger.info('Checking number of jvm processes not related to HDFS daemons running '
                'on the workers.')
    print_run_command_output(hadoop_workers.run_sync(
        'ps aux | grep java| egrep -v "JournalNode|DataNode|NodeManager" | grep -v egrep | wc -l'))

    ask_confirmation(
        'If there are remaining jvm processes running on the Cluster, please kill them.')

    logger.info('Checking HDFS master/standby status.')

    hadoop_master_hdfs_service = hadoop_master.hosts[0].replace('.', '-')
    hadoop_standby_hdfs_service = hadoop_standby.hosts[0].replace('.', '-')

    print_run_command_output(hadoop_master.run_sync(
        'kerberos-run-command hdfs /usr/bin/hdfs haadmin -getServiceState ' +
        hadoop_master_hdfs_service))
    print_run_command_output(hadoop_master.run_sync(
        'kerberos-run-command hdfs /usr/bin/hdfs haadmin -getServiceState ' +
        hadoop_standby_hdfs_service))

    ask_confirmation('Please make sure that the active/standby nodes are correct.')

    logger.info("Entering HDFS Safe Mode.")
    hadoop_master.run_sync(
        'kerberos-run-command hdfs hdfs dfsadmin -safemode enter',
        'kerberos-run-command hdfs hdfs dfsadmin -saveNamespace')

    logger.info("Backup of the Namenode's state..")
    hadoop_master.run_sync(
        'tar -cvf /root/hadoop-namedir-backup-stop-cluster-cookbook-$(date +%s).tar /var/lib/hadoop/name')
    print_run_command_output(hadoop_master.run_sync(
        'ls -lh /root/hadoop-namedir-backup-stop-cluster-cookbook*'))
    logger.info("Safety checks completed, starting the procedure.")


def run(args, spicerack):
    """Restart all Hadoop jvm daemons on a given cluster"""
    if args.cluster == 'test':
        suffix = '-test'
    elif args.cluster == 'analytics':
        suffix = ''
    else:
        raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

    cluster_alias = CLUSTER_CUMIN_ALIAS + suffix
    master_alias = MASTER_CUMIN_ALIAS + suffix
    standby_alias = STANDBY_CUMIN_ALIAS + suffix
    workers_alias = WORKERS_CUMIN_ALIAS + suffix
    hdfs_jn_alias = HDFS_JOURNAL_CUMIN_ALIAS + suffix

    ensure_shell_is_durable()

    hadoop_hosts = spicerack.remote().query(cluster_alias)
    hadoop_hdfs_journal_workers = spicerack.remote().query(hdfs_jn_alias)
    hadoop_workers = spicerack.remote().query(workers_alias)
    hadoop_master = spicerack.remote().query(master_alias)
    hadoop_standby = spicerack.remote().query(standby_alias)

    safety_checks(hadoop_workers, hadoop_master, hadoop_standby)

    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Stop the Hadoop cluster before maintenance.')
    puppet = spicerack.puppet(hadoop_hosts)
    puppet.disable(reason)

    with icinga.hosts_downtimed(hadoop_hosts.hosts, reason,
                                duration=timedelta(minutes=120)):

        logger.info("Stopping all Yarn daemons.")
        hadoop_workers.run_sync(
            'systemctl stop hadoop-yarn-nodemanager',
            batch_size=5)

        hadoop_standby.run_sync(
            'systemctl stop hadoop-yarn-resourcemanager')

        logger.info('Sleeping some seconds to let things to stabilize')
        time.sleep(10)

        hadoop_master.run_sync(
            'systemctl stop hadoop-yarn-resourcemanager')

        logger.info(
            "Stopping all HDFS Datanodes. Be patient, very slow step."
            "Two nodes at the time, one minute sleep between each batch.")
        hadoop_workers.run_sync(
            'systemctl stop hadoop-hdfs-datanode',
            batch_size=2, batch_sleep=30.0)

        logger.info('Stopping HDFS Standby Namenode.')
        hadoop_standby.run_sync(
            'systemctl stop hadoop-hdfs-namenode',
            'systemctl stop hadoop-hdfs-zkfc')

        logger.info('Sleeping one minute to let things to stabilize')
        time.sleep(60)

        logger.info('Stopping HDFS Master Namenode.')
        hadoop_standby.run_sync(
            'systemctl stop hadoop-hdfs-namenode',
            'systemctl stop hadoop-hdfs-zkfc')

        logger.info('Stopping MapReduce History Server.')
        hadoop_standby.run_sync(
            'systemctl stop hadoop-mapreduce-historyserver')

        logger.info('Stopping HDFS Journalnodes.')
        hadoop_hdfs_journal_workers.run_sync(
            'systemctl stop hadoop-hdfs-journalnode',
            batch_size=1, batch_sleep=30.0)

        print_run_command_output(hadoop_hosts.run_sync(
            'ps aux | grep java | grep -v grep | wc -l'))

        logger.info('If there are remaining jvm processes running on the Cluster, please check them.')

    logger.warning('As outlined before, puppet has been left disabled on all the Hadoop hosts '
                   'to prevent daemons to be restarted before time.')
    logger.info('The procedure is completed.')
