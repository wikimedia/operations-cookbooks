"""Reboot all worker nodes in a given Hadoop cluster."""
import logging
import math
import time

from datetime import datetime, timedelta

from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre.hadoop import HADOOP_CLUSTER_NAMES


__title__ = 'Reboot Hadoop worker nodes'
logger = logging.getLogger(__name__)


class RebootHadoopWorkers(CookbookBase):
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
    - wait some minutes to give a chance to the jvm containers to finish (if they
      don't it is not a big problem, jobs in Hadoop can be rescheduled).
    - stop the HDFS datanode on the host (safer that abruptively reboot, from the point
      of view of corrupted HDFS blocks).
    - stop the HDFS journalnode (if running on the host).
    - reboot
    - wait for the host to boot
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
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
        parser.add_argument('--workers-cumin-query', required=False, help='A cumin query string to select '
                            'the Hadoop workers to work on. This overrides the selection of the '
                            'cluster argument. It should used be when only a few hosts need to be rebooted.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootHadoopWorkersRunner(args, self.spicerack)


class RebootHadoopWorkersRunner(CookbookRunnerBase):
    """Reboot Hadoop workers runner"""

    def __init__(self, args, spicerack):
        """Reboot all workers of a given Hadoop cluster."""
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
        self.spicerack_remote = spicerack.remote()
        self.spicerack = spicerack
        self.reboot_batch_size = args.batch_size
        self.yarn_nm_sleep_seconds = args.yarn_nm_sleep_seconds
        self.workers_cumin_query = args.workers_cumin_query
        self.reason = spicerack.admin_reason('Reboot.')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Hadoop {} cluster'.format(self.cluster)

    def _reboot_hadoop_workers(self, hadoop_workers_batch, stop_journal_daemons=False):
        """Reboot a batch of Hadoop workers"""
        with self.spicerack.alerting_hosts(hadoop_workers_batch.hosts).downtimed(
                self.reason, duration=timedelta(minutes=60)):
            puppet = self.spicerack.puppet(hadoop_workers_batch)
            puppet.disable(self.reason)
            logger.info('Stopping the Yarn Nodemanagers...')
            confirm_on_failure(
                hadoop_workers_batch.run_sync, 'systemctl stop hadoop-yarn-nodemanager')
            logger.info(
                'Wait %s seconds to allow jvm containers to finish..', self.yarn_nm_sleep_seconds)
            time.sleep(self.yarn_nm_sleep_seconds)
            logger.info('Stopping the HDFS Datanodes...')
            confirm_on_failure(
                hadoop_workers_batch.run_sync, 'systemctl stop hadoop-hdfs-datanode')
            if stop_journal_daemons:
                logger.info('Stopping the HDFS Journalnode...')
                confirm_on_failure(
                    hadoop_workers_batch.run_sync, 'systemctl stop hadoop-hdfs-journalnode')
            logger.info('Rebooting hosts..')
            reboot_time = datetime.utcnow()
            confirm_on_failure(
                hadoop_workers_batch.reboot,
                batch_size=len(hadoop_workers_batch.hosts),
                batch_sleep=None)
            confirm_on_failure(
                hadoop_workers_batch.wait_reboot_since, reboot_time)
            puppet.enable(self.reason)

    def run(self):
        """Reboot all Hadoop workers of a given cluster"""
        if self.workers_cumin_query:
            hadoop_workers = self.spicerack_remote.query(self.cluster_cumin_alias)
            hadoop_workers_override = self.spicerack_remote.query(self.workers_cumin_query)
            hadoop_workers = self.spicerack_remote.query(
                "D{{{}}}".format(hadoop_workers.hosts.intersection(hadoop_workers_override.hosts)))
            ask_confirmation(
                'The user chose to limit the number of Hadoop workers to reboot. '
                'This option does not care about Journal nodes and it will only reboot '
                'hosts following the batch size ({}). This means that more than one Journal node '
                'may potentially be rebooted at the same time. Please check the list of hosts ({}) '
                'before proceeding: {}'.format(self.reboot_batch_size, len(hadoop_workers), hadoop_workers))

            worker_hostnames_n_slices = math.floor(len(hadoop_workers.hosts) / self.reboot_batch_size)
            logger.info('Rebooting Hadoop workers')
            for hadoop_workers_batch in hadoop_workers.split(worker_hostnames_n_slices):
                logger.info("Currently processing: %s", hadoop_workers_batch.hosts)
                self._reboot_hadoop_workers(hadoop_workers_batch)

        else:
            # The test cluster have few worker nodes, all running HDFS Datanodes
            # and Journalnodes, so we need a simpler procedure for this use case.
            if self.cluster != 'test':
                hadoop_workers_no_journal = self.spicerack_remote.query(
                    self.cluster_cumin_alias + ' and not ' + self.hdfs_jn_cumin_alias)

                # Split the workers into batches of hostnames
                worker_hostnames_n_slices = math.floor(len(hadoop_workers_no_journal.hosts) / self.reboot_batch_size)

                logger.info('Rebooting Hadoop workers NOT running a HDFS Journalnode')
                for hadoop_workers_batch in hadoop_workers_no_journal.split(worker_hostnames_n_slices):
                    logger.info("Currently processing: %s", hadoop_workers_batch.hosts)
                    self._reboot_hadoop_workers(hadoop_workers_batch)

            logger.info('Rebooting Hadoop workers running a HDFS Journalnode')
            # Using the following loop to iterate over every HDFS JournalNode
            # one at the time.
            hadoop_hdfs_journal_workers = self.spicerack_remote.query(self.hdfs_jn_cumin_alias)
            for hadoop_workers_batch in hadoop_hdfs_journal_workers.split(len(hadoop_hdfs_journal_workers.hosts)):
                logger.info("Currently processing: %s", hadoop_workers_batch.hosts)
                self._reboot_hadoop_workers(hadoop_workers_batch, stop_journal_daemons=True)

        logger.info('All reboots completed!')
