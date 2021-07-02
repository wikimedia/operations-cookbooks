"""Restart all Hadoop jvm daemons on master hosts.

The idea for this cookbook is to:
- Set Icinga downtime for all nodes (no puppet disable or depool is needed).
- Check the status of Yarn Resourcemanager daemons (expecting an active and a standby node).
- Check the status of HDFS Namenode daemons (expecting an active and a standby node).
- Restart one Resource Manager at the time.
- Force a failover of HDFS Namenode to the standby node.
- Restart one HDFS Namenode (the current standby).
- Force a failover of HDFS Namenode to the standby node (basically restoring the prev. state).
- Restart the Mapreduce history server (only on one host).
"""
import argparse
import logging
import time

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from . import HADOOP_CLUSTER_NAMES


__title__ = 'Restart all the jvm daemons on Hadoop master nodes'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                        choices=HADOOP_CLUSTER_NAMES)
    parser.add_argument('--yarn-rm-sleep-seconds', type=float, default=60.0,
                        help="Seconds to sleep between Yarn Resourcemanager restarts.")
    parser.add_argument('--hdfs-nn-sleep-seconds', type=float, default=600.0,
                        help="Seconds to sleep between HDFS Namenode restarts.")

    return parser


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


def run(args, spicerack):
    """Restart all Hadoop jvm daemons on a given cluster"""
    if args.cluster == 'test':
        suffix = '-test'
    elif args.cluster == 'analytics':
        suffix = ''
    else:
        raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

    ensure_shell_is_durable()

    hadoop_master_cumin_alias = 'A:hadoop-master' + suffix
    hadoop_standby_cumin_alias = 'A:hadoop-standby' + suffix

    hadoop_master = spicerack.remote().query(hadoop_master_cumin_alias)
    hadoop_standby = spicerack.remote().query(hadoop_standby_cumin_alias)
    icinga_hosts = spicerack.icinga_hosts(hadoop_master.hosts | hadoop_standby.hosts)
    reason = spicerack.admin_reason('Restart of jvm daemons.')

    yarn_rm_sleep = args.yarn_rm_sleep_seconds
    hdfs_nn_sleep = args.hdfs_nn_sleep_seconds

    # Safety checks
    if hdfs_nn_sleep < 600:
        ask_confirmation('The HDFS Namenode restart sleep is less than 600s, are you sure?')
    if yarn_rm_sleep < 60:
        ask_confirmation('The Yarn Resourcemanager restart sleep is less than 60s, are you sure?')

    logger.info('Checking HDFS and Yarn daemon status. We expect active statuses on the Master node, '
                'and standby statuses on the other. Please do not proceed otherwise.')

    # This is needed due to the format of the hostname in the command, for example:
    # sudo -u hdfs /usr/bin/hdfs haadmin -getServiceState an-master1001-eqiad-wmnet
    hadoop_master_service = hadoop_master.hosts[0].replace('.', '-')
    hadoop_standby_service = hadoop_standby.hosts[0].replace('.', '-')

    print_hadoop_service_state(
        hadoop_master, hadoop_master_service, hadoop_standby_service)

    ask_confirmation('Please make sure that the active/standby nodes are correct.')

    with icinga_hosts.downtimed(reason, duration=timedelta(minutes=120)):
        logger.info("Restarting Yarn Resourcemanager on Master.")
        hadoop_master.run_sync('systemctl restart hadoop-yarn-resourcemanager')
        logger.info("Sleeping %s seconds.", yarn_rm_sleep)
        time.sleep(yarn_rm_sleep)
        logger.info("Restarting Yarn Resourcemanager on Standby.")
        hadoop_standby.run_sync('systemctl restart hadoop-yarn-resourcemanager')

        print_hadoop_service_state(
            hadoop_master, hadoop_master_service, hadoop_standby_service, hdfs=False)

        ask_confirmation("Ok to proceed with HDFS Namenodes ?")

        logger.info("Run manual HDFS failover from master to standby.")
        run_hdfs_namenode_failover(hadoop_master, hadoop_master_service, hadoop_standby_service)

        logger.info("Sleeping 30 seconds.")
        time.sleep(30)

        logger.info("Restart HDFS Namenode on the master.")
        hadoop_master.run_async(
            'systemctl restart hadoop-hdfs-zkfc',
            'systemctl restart hadoop-hdfs-namenode')

        logger.info("Sleeping %s seconds.", hdfs_nn_sleep)
        time.sleep(hdfs_nn_sleep)

        print_hadoop_service_state(
            hadoop_master, hadoop_master_service, hadoop_standby_service, yarn=False)

        ask_confirmation("Ok to proceed?")

        logger.info("Run manual HDFS failover from standby to master.")
        run_hdfs_namenode_failover(hadoop_master, hadoop_standby_service, hadoop_master_service)

        logger.info("Sleeping 30 seconds.")
        time.sleep(30)

        logger.info("Restart HDFS Namenode on the standby.")
        hadoop_standby.run_async(
            'systemctl restart hadoop-hdfs-zkfc',
            'systemctl restart hadoop-hdfs-namenode')

        logger.info("Sleeping %s seconds.", hdfs_nn_sleep)
        time.sleep(hdfs_nn_sleep)

        logger.info("\n\nSummary of active/standby statuses after the restarts:")

        print_hadoop_service_state(
            hadoop_master, hadoop_master_service, hadoop_standby_service)

        logger.info("Restart MapReduce historyserver on the master.")
        hadoop_master.run_sync('systemctl restart hadoop-mapreduce-historyserver')

    logger.info("All jvm restarts completed!")
