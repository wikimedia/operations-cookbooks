"""Stop an Hadoop cluster."""
import logging
import time

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from . import (HADOOP_CLUSTER_NAMES, CLUSTER_CUMIN_ALIAS,
               MASTER_CUMIN_ALIAS, STANDBY_CUMIN_ALIAS,
               WORKERS_CUMIN_ALIAS, HDFS_JOURNAL_CUMIN_ALIAS)

logger = logging.getLogger(__name__)


class StopHadoop(CookbookBase):
    """Gracefully stop an Hadoop cluster.

    This cookbook should be used when some important maintenance is
    needed to be performed, like upgrading the package distribution.
    The cookbook takes care of gracefully shutdown the hadoop cluster.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                            choices=HADOOP_CLUSTER_NAMES)
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return StopHadoopRunner(args, self.spicerack)


class StopHadoopRunner(CookbookRunnerBase):
    """Stop Hadoop cluster runner"""

    def __init__(self, args, spicerack):
        """Gracefully stop an Hadoop cluster."""
        if args.cluster == 'test':
            suffix = '-test'
        elif args.cluster == 'backup':
            suffix = '-backup'
        elif args.cluster == 'analytics':
            suffix = ''
        else:
            raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

        self.cluster = args.cluster

        cluster_alias = CLUSTER_CUMIN_ALIAS + suffix
        master_alias = MASTER_CUMIN_ALIAS + suffix
        standby_alias = STANDBY_CUMIN_ALIAS + suffix
        workers_alias = WORKERS_CUMIN_ALIAS + suffix
        hdfs_jn_alias = HDFS_JOURNAL_CUMIN_ALIAS + suffix

        self.hadoop_hosts = spicerack.remote().query(cluster_alias)
        self.hadoop_hdfs_journal_workers = spicerack.remote().query(hdfs_jn_alias)
        self.hadoop_workers = spicerack.remote().query(workers_alias)
        self.hadoop_master = spicerack.remote().query(master_alias)
        self.hadoop_standby = spicerack.remote().query(standby_alias)

        self.alerting_hosts = spicerack.alerting_hosts(self.hadoop_hosts.hosts)
        self.admin_reason = spicerack.admin_reason('Stop the Hadoop cluster before maintenance.')
        self.puppet = spicerack.puppet(self.hadoop_hosts)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Hadoop {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def safety_checks(self):
        """Run safety checks before starting any invasive action in the cluster."""
        ask_confirmation(
            'This cookbook takes care only about the Hadoop master/standby/worker nodes, '
            'so please make sure that coordinator, presto, druid, notebooks, clients, etc.. '
            'are all downtimed and with puppet disabled.\n'
            'Important things to check/remember:\n'
            '- systemd timers running jobs need to be stopped.\n'
            '- oozie/hive/presto need to be shutdown on the coordinator node.\n'
            '- /mnt/hdfs mountpoints need to be unmounted from clients.\n'
            '- Hue/Jupyter processes should be down.\n'
            '- The Cluster needs to be drained from running jobs.\n\n'
            'Also please note that the Hadoop cluster\'s hosts will be left with puppet '
            'disabled, to prevent daemons to be restarted before time.')

        logger.info('Checking number of jvm processes not related to HDFS daemons running '
                    'on the workers.')
        self.hadoop_workers.run_sync(
            'ps aux | grep [j]ava| egrep -v "JournalNode|DataNode|NodeManager" | grep -v egrep | wc -l')

        ask_confirmation(
            'If there are remaining jvm processes running on the Cluster, please kill them.')

        logger.info('Checking HDFS master/standby status.')

        hadoop_master_hdfs_service = self.hadoop_master.hosts[0].replace('.', '-')
        hadoop_standby_hdfs_service = self.hadoop_standby.hosts[0].replace('.', '-')

        logger.info('HDFS Master status:')
        self.hadoop_master.run_sync(
            'kerberos-run-command hdfs /usr/bin/hdfs haadmin -getServiceState ' +
            hadoop_master_hdfs_service)

        logger.info('HDFS Standby status:')
        self.hadoop_master.run_sync(
            'kerberos-run-command hdfs /usr/bin/hdfs haadmin -getServiceState ' +
            hadoop_standby_hdfs_service)

        ask_confirmation('Please make sure that the active/standby nodes are correct.')

        logger.info("Entering HDFS Safe Mode.")
        self.hadoop_master.run_sync(
            'kerberos-run-command hdfs hdfs dfsadmin -safemode enter',
            'kerberos-run-command hdfs hdfs dfsadmin -saveNamespace')

        logger.info("Backup of the Namenode's state.")
        self.hadoop_master.run_sync(
            'tar -cvf /root/hadoop-namedir-backup-stop-cluster-cookbook-$(date +%s).tar /var/lib/hadoop/name')
        self.hadoop_master.run_sync(
            'ls -lh /root/hadoop-namedir-backup-stop-cluster-cookbook*')
        logger.info("Safety checks completed, starting the procedure.")

    def run(self):
        """Restart all Hadoop jvm daemons on a given cluster"""
        ensure_shell_is_durable()
        self.safety_checks()

        self.puppet.disable(self.admin_reason)

        self.alerting_hosts.downtime(self.admin_reason, duration=timedelta(minutes=120))

        logger.info("Stopping all Yarn daemons.")
        self.hadoop_workers.run_sync(
            'systemctl stop hadoop-yarn-nodemanager',
            batch_size=5)

        self.hadoop_standby.run_sync(
            'systemctl stop hadoop-yarn-resourcemanager')

        logger.info('Sleeping some seconds to let things to stabilize.')
        time.sleep(10)

        self.hadoop_master.run_sync(
            'systemctl stop hadoop-yarn-resourcemanager')

        logger.info(
            "Stopping all HDFS Datanodes. Be patient, very slow step. "
            "Two nodes at the time, one minute sleep between each batch.")
        self.hadoop_workers.run_sync(
            'systemctl stop hadoop-hdfs-datanode',
            batch_size=2, batch_sleep=30.0)

        logger.info('Stopping HDFS Standby Namenode.')
        self.hadoop_standby.run_sync(
            'systemctl stop hadoop-hdfs-namenode',
            'systemctl stop hadoop-hdfs-zkfc')

        logger.info('Sleeping one minute to let things to stabilize')
        time.sleep(60)

        logger.info('Stopping HDFS Master Namenode.')
        self.hadoop_master.run_sync(
            'systemctl stop hadoop-hdfs-namenode',
            'systemctl stop hadoop-hdfs-zkfc')

        logger.info('Stopping MapReduce History Server.')
        self.hadoop_master.run_sync(
            'systemctl stop hadoop-mapreduce-historyserver')

        logger.info('Stopping HDFS Journalnodes.')
        self.hadoop_hdfs_journal_workers.run_sync(
            'systemctl stop hadoop-hdfs-journalnode',
            batch_size=1, batch_sleep=30.0)

        logger.info("Backup of the Journalnodes' state.")
        self.hadoop_hdfs_journal_workers.run_sync(
            'tar -cvf /root/hadoop-journaldir-backup-stop-cluster-cookbook-$(date +%s).tar /var/lib/hadoop/journal')
        self.hadoop_hdfs_journal_workers.run_sync('ls -lh /root/hadoop-journaldir-backup-stop-cluster-cookbook*')

        self.hadoop_hosts.run_sync('ps aux | grep java | grep -v grep | wc -l')

        logger.info('If there are remaining jvm processes running on the Cluster, please check them.')

        logger.warning('As outlined before, puppet has been left disabled on all the Hadoop hosts '
                       'to prevent daemons to be restarted before time.')
        logger.info('The procedure is completed.')
