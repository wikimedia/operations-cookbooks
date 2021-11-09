"""Reboot all Presto nodes in a cluster."""
import argparse
import logging

from datetime import datetime, timedelta
from time import sleep

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter

logger = logging.getLogger(__name__)


class RebootPrestoWorkers(CookbookBase):
    """Reboot all Presto nodes in a cluster.

    This cookbook should be used, for example,
    to upgrade the kernel while keeping the cluster online.

    The presto workers are all stateless daemons that can be rebooted anytime,
    but for availability purposes we want to limit the number of reboots to one.

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Presto cluster to work on.',
                            choices=['analytics'])
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootPrestoWorkersRunner(args, self.spicerack)


class RebootPrestoWorkersRunner(CookbookRunnerBase):
    """Reboot Presto cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Reboot Presto on a given cluster."""
        ensure_shell_is_durable()

        self.icinga_hosts = spicerack.icinga_hosts
        self.puppet = spicerack.puppet
        self.admin_reason = spicerack.admin_reason('Reboot Presto nodes')
        self.remote = spicerack.remote()

        self.cluster = args.cluster

        cluster_cumin_alias = 'A:presto-' + self.cluster

        self.presto_workers = self.remote.query(cluster_cumin_alias)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Presto {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def _reboot_presto_node(self, host):
        """Reboot a single Presto node."""
        node = self.remote.query('D{' + host + '}')
        puppet = self.puppet(node)
        duration = timedelta(minutes=120)

        with self.icinga_hosts([host]).downtimed(self.admin_reason, duration=duration):
            with puppet.disabled(self.admin_reason):
                logger.info('Stopping the Presto worker daemon..')
                node.run_async('systemctl stop presto-server')
                reboot_time = datetime.utcnow()
                node.reboot()
                node.wait_reboot_since(reboot_time)

    def run(self):
        """Reboot all Presto nodes in a given cluster"""
        for host in self.presto_workers.hosts:
            logger.info('Start reboot of Presto node %s', host)
            self._reboot_presto_node(host)
            logger.info('Reboot completed for node %s. Waiting 2 mins before proceeding.', host)
            sleep(120)

        logger.info('All Presto node reboots completed!')
