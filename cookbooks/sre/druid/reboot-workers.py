"""Reboot all Druid nodes in a cluster."""
import argparse
import logging

from datetime import datetime, timedelta
from time import sleep

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.druid import DRUID_DAEMONS


logger = logging.getLogger(__name__)


class RebootDruidWorkers(CookbookBase):
    """Reboot all Druid nodes in a cluster.

    Takes care to gracefully shut down zookeeper and all Druid daemons
    before rebooting the node.

    This cookbook should be used, for example,
    to upgrade the kernel while keeping the cluster online.

    Assumptions:
    - Before running this cookbook, the zookeeper cluster should be in a consistent state
        (one leader node and the rest are followers). The cookbook will print the cluster state
        and ask for confirmation of the cluster status before continuing.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Druid cluster to work on.',
                            choices=['public', 'analytics', 'test'])

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootDruidWorkersRunner(args, self.spicerack)


class RebootDruidWorkersRunner(CookbookRunnerBase):
    """Reboot druid cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Reboot Druid on a given cluster."""
        ensure_shell_is_durable()

        self.icinga_hosts = spicerack.icinga_hosts  # Store the method to be called on each host
        self.puppet = spicerack.puppet
        self.spicerack = spicerack
        self.admin_reason = spicerack.admin_reason('Reboot Druid nodes')
        self.remote = spicerack.remote()

        self.cluster = args.cluster

        cluster_cumin_alias = 'A:druid-' + self.cluster

        self.druid_workers = self.remote.query(cluster_cumin_alias)

        self.need_depool = self.cluster == 'public'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Druid {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def reboot_druid_node(self, host):
        """Reboot a single Druid node."""
        node = self.remote.query('D{' + host + '}')
        puppet = self.puppet(node)

        stop_process_commands = []
        for daemon in DRUID_DAEMONS:
            stop_process_commands.append('systemctl stop druid-' + daemon)
            stop_process_commands.append('sleep 10')

        duration = timedelta(minutes=120)

        with self.icinga_hosts([host]).downtimed(self.admin_reason, duration=duration):
            with puppet.disabled(self.admin_reason):
                logger.info('Stopping active zookeeper on host %s', host)
                node.run_sync('systemctl --quiet is-active zookeeper && systemctl stop zookeeper || exit 0')

                if self.need_depool:
                    node.run_sync('depool')

                node.run_async(*stop_process_commands)

                reboot_time = datetime.utcnow()
                node.reboot()
                node.wait_reboot_since(reboot_time)

                if self.need_depool:
                    node.run_sync('pool')

    def run(self):
        """Reboot all Druid nodes in a given cluster"""
        self.druid_workers.run_async(
            'systemctl --quiet is-active zookeeper && echo stats | nc localhost 2181 | grep Mode || exit 0'
        )

        ask_confirmation(
            'From the output of the last command, please check the status of the'
            ' Zookeeper cluster before proceeding.'
            ' There must be only one leader and the rest must be followers.'
        )

        for host in self.druid_workers.hosts:
            logger.info('Start reboot of druid node %s', host)
            self.reboot_druid_node(host)
            logger.info('Reboot completed for node %s. Waiting 10 minutes for daemons to catch up', host)
            sleep(600)

        logger.info('All Druid node reboots completed!')
