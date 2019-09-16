"""Reboot all Hadoop worker hosts.

The idea for this cookbook is to separate the hosts to reboot in:

- hosts running a HDFS journalnode (usually 3 or 5 hosts in total)
- rest of the hosts

The first group is going to be rebooted one host maximum at the time, with
a slower pace. The second can be rebooted including more hosts in the same batch,
with higher pace.

For each host, the procedure should be to:
- disable puppet
- stop the Yarn Namenode, to avoid any job to schedule jvm containers on the host
  (a sort of hacky drain procedure).
- wait some minutes to give a chance to the the jvm containers to finish (if they
  don't it is not a big problem, jobs in Hadoop can be rescheduled).
- stop the HDFS datanode on the host (safer that abruptively reboot, from the point
  of view of corrupted HDFS blocks).
- stop the HDFS journalnode (if running on the host).
- reboot
- wait for the host to boot
"""
import argparse
import logging
import math
import time

from datetime import datetime, timedelta
from spicerack.interactive import ask_confirmation, ensure_shell_is_durable
from spicerack.remote import RemoteCheckError, RemoteExecutionError
from . import HADOOP_CLUSTER_NAMES

__title__ = 'Reboot Hadoop worker nodes'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                        choices=HADOOP_CLUSTER_NAMES)
    parser.add_argument('--yarn-nm-sleep-seconds', type=float, default=600.0,
                        help='Seconds to sleep after stopping the Yarn Nodemanager.')
    parser.add_argument('--reboot-batch-sleep-seconds', type=float, default=600.0,
                        help='Seconds to sleep between each batch of reboots. '
                             'The batches will not apply to hosts running '
                             'a HDFS Journalnode (that will be rebooted strictly '
                             'one at the time).')
    parser.add_argument('--batch-size', type=int, default=2,
                        help='Size of each batch of reboots.')

    return parser


def reboot_hadoop_workers(hadoop_workers_batch, yarn_nm_sleep_seconds,
                          spicerack, icinga):
    """Reboot a batch of Hadoop workers"""
    reason = spicerack.admin_reason('Reboot.')
    with icinga.hosts_downtimed(hadoop_workers_batch.hosts, reason,
                                duration=timedelta(minutes=60)):
        puppet = spicerack.puppet(hadoop_workers_batch)
        puppet.disable(reason)
        logger.info('Stopping the Yarn Nodemanagers...')
        hadoop_workers_batch.run_sync('systemctl stop hadoop-yarn-nodemanager')
        logger.info(
            'Wait %s seconds to allow jvm containers to finish..', yarn_nm_sleep_seconds)
        time.sleep(yarn_nm_sleep_seconds)
        logger.info('Stopping the HDFS Datanodes...')
        hadoop_workers_batch.run_sync('systemctl stop hadoop-hdfs-datanode')
        logger.info('Rebooting hosts..')
        reboot_time = datetime.utcnow()
        hadoop_workers_batch.reboot(
            batch_size=len(hadoop_workers_batch.hosts),
            batch_sleep=None)
        try:
            hadoop_workers_batch.wait_reboot_since(reboot_time)
        except (RemoteCheckError, RemoteExecutionError):
            logger.exception('Failure registered while rebooting...')
            ask_confirmation('Do you wish to continue rebooting? '
                             'Please note that saying no will require some follow-up, '
                             'like re-enable puppet manually.')
        puppet.enable(reason)


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

    spicerack_remote = spicerack.remote()
    icinga = spicerack.icinga()
    hadoop_workers_no_journal = spicerack_remote.query(
        cluster_cumin_alias + ' and not ' + hdfs_jn_cumin_alias)
    hadoop_hdfs_journal_workers = spicerack_remote.query(hdfs_jn_cumin_alias)

    reboot_batch_size = args.batch_size
    yarn_nm_sleep_seconds = args.yarn_nm_sleep_seconds

    # Split the workers into batches of hostnames
    worker_hostnames_n_slices = math.floor(len(hadoop_workers_no_journal.hosts) / reboot_batch_size)

    logger.info('Rebooting Hadoop workers NOT running a HDFS Journalnode')
    for hadoop_workers_batch in hadoop_workers_no_journal.split(worker_hostnames_n_slices):
        logger.info("Currently processing: %s", hadoop_workers_batch.hosts)
        reboot_hadoop_workers(hadoop_workers_batch, yarn_nm_sleep_seconds, spicerack, icinga)

    logger.info('Rebooting Hadoop workers running a HDFS Journalnode')
    # Using the following loop to iterate over every HDFS JournalNode
    # one at the time.
    for hadoop_workers_batch in hadoop_hdfs_journal_workers.split(len(hadoop_hdfs_journal_workers.hosts)):
        logger.info("Currently processing: %s", hadoop_workers_batch.hosts)
        reboot_hadoop_workers(hadoop_workers_batch, yarn_nm_sleep_seconds, spicerack, icinga)

    logger.info('All reboots completed!')
