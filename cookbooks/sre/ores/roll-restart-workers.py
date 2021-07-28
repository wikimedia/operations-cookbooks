"""Restart ORES daemons in a cluster"""
import argparse
import logging
import time

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.ores import ORES_DAEMONS


logger = logging.getLogger(__name__)


class RestartOresWorkers(CookbookBase):
    """Roll restart all ORES daemons in a cluster.

    Every Ores worker host manages multiple daemons:
    * uwsgi
    * celery

    This cookbook, for every ORES node in a cluster:
    - Depool the node
    - Restart uwsgi and celery very gently,
    - Re-pool the node.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(
            description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the ORES cluster to work on.',
                            choices=['eqiad', 'codfw', 'canary'])
        parser.add_argument('--daemons', help='The daemons to restart.', nargs='+',
                            default=ORES_DAEMONS,
                            choices=ORES_DAEMONS)
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestartOresWorkersRunner(args, self.spicerack)


class RestartOresWorkersRunner(CookbookRunnerBase):
    """Restart ORES daemons on a given cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Restart ORES daemons on a given cluster."""
        cluster_cumin_alias = "A:ores-" + args.cluster
        self.cluster = args.cluster
        self.ores_workers = spicerack.remote().query(cluster_cumin_alias)
        self.icinga_hosts = spicerack.icinga_hosts(self.ores_workers.hosts)
        self.reason = spicerack.admin_reason('Roll restart of ORES\'s daemons.')
        self.daemons = args.daemons
        self.spicerack = spicerack
        self.confctl = spicerack.confctl('node')
        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for ORES {} cluster: {}'.format(self.cluster, self.reason)

    def run(self):
        """Restart all ORES daemons on a given cluster"""
        with self.icinga_hosts.downtimed(self.reason, duration=timedelta(minutes=60)):
            logger.info(
                'Restarting daemons (%s), one host at the time.', ','.join(self.daemons))
            for host in self.ores_workers.hosts:
                with self.confctl.change_and_revert('pooled', 'yes', 'no', name=host):
                    remote_host = self.spicerack.remote().query('D{{{h}}}'.format(h=str(host)))
                    for daemon in self.daemons:
                        logger.info('Restarting %s on %s', daemon, str(host))
                        remote_host.run_sync('systemctl restart ' + daemon)
                        time.sleep(60)

        logger.info("All ORES restarts completed!")
