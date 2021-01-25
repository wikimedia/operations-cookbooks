"""Upgrade/Rollback Hadoop to a newer/previous distribution on client nodes."""

import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks import ArgparseFormatter
from cookbooks.sre.hadoop import (HADOOP_CLUSTER_NAMES, HADOOP_CLIENT_CUMIN_ALIASES,
                                  HADOOP_TEST_CLIENT_CUMIN_ALIASES, CDH_PACKAGES_NOT_IN_BIGTOP)

logger = logging.getLogger(__name__)


class ChangeHadoopDistroOnClients(CookbookBase):
    """Change Hadoop distribution on clients.

    This cookbook should be used when there is the need to upgrade/rollback
    Hadoop packages on client nodes. The term 'client' is a bit broad in this
    cookbook, because it means any non-hadoop worker/master/standby node that
    needs a Hadoop deb packages upgrade.
    The assumption is that the Hadoop cluster has already been upgraded via its
    related cookbook.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('cluster', help='The name of the Hadoop cluster to work on.',
                            choices=HADOOP_CLUSTER_NAMES)
        parser.add_argument('--cumin-client-label', required=False, help='A cumin client label to select '
                            'the Hadoop clients to work on. This limits/overrides the selection of the '
                            'cluster argument.')
        parser.add_argument('--rollback', action='store_true',
                            help="Set the cookbook to run rollback commands.")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ChangeHadoopDistroOnClientsRunner(args, self.spicerack)


class ChangeHadoopDistroOnClientsRunner(CookbookRunnerBase):
    """Change Hadoop distribution on clients cookbook runner."""

    def __init__(self, args, spicerack):
        """Change Hadoop distribution on all the clients of a given cluster"""
        if args.cluster == 'test':
            cumin_labels = HADOOP_TEST_CLIENT_CUMIN_ALIASES
        elif args.cluster == 'analytics':
            cumin_labels = HADOOP_CLIENT_CUMIN_ALIASES
        else:
            raise RuntimeError("Hadoop cluster {} not supported.".format(args.cluster))

        ensure_shell_is_durable()

        spicerack_remote = spicerack.remote()
        if args.cumin_client_label:
            if args.cumin_client_label not in cumin_labels:
                raise RuntimeError(
                    "Cumin label {} not supported. Please use one of: {}"
                    .format(args.cumin_client_label, cumin_labels))
            cumin_labels = [args.cumin_client_label]

        self.icinga = spicerack.icinga()
        self.hadoop_client_hosts = spicerack_remote.query(' or '.join(cumin_labels))
        self.reason = spicerack.admin_reason('Change Hadoop distribution')
        self.rollback = args.rollback
        self.cluster = args.cluster

        ask_confirmation(
            "This cookbook assumes that the Hadoop cluster runs already the new distro, "
            "please do not proceed otherwise.")

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Hadoop {} cluster: {}'.format(self.cluster, self.reason)

    def _remove_packages(self):
        """Remove all Hadoop packages on the client"""
        logger.info('Removing the Hadoop packages on all nodes.')
        confirm_on_failure(
            self.hadoop_client_hosts.run_async,
            "apt-get remove -y `cat /root/cdh_package_list`")

    def _install_packages_on_clients(self):
        """Install Hadoop packages on Hadoop client nodes."""
        logger.info("Install packages on worker nodes (long step).")

        if self.rollback:
            confirm_on_failure(
                self.hadoop_client_hosts.run_sync,
                'apt-get install -y `cat /root/cdh_package_list`')
        else:
            apt_package_filter = "|".join(CDH_PACKAGES_NOT_IN_BIGTOP)
            confirm_on_failure(
                self.hadoop_client_hosts.run_sync,
                "apt-get install -y `cat /root/cdh_package_list | tr ' ' '\n' | "
                f"egrep -v '{apt_package_filter}' | tr '\n' ' '`")

    def run(self):
        """Change the Hadoop distribution."""
        with self.icinga.hosts_downtimed(self.hadoop_client_hosts.hosts, self.reason,
                                         duration=timedelta(minutes=30)):
            if not self.rollback:
                logger.info(
                    'Saving a snapshot of cdh package names and versions in /root/cdh_package_list '
                    'on all nodes, and removing all packages.')
                confirm_on_failure(
                    self.hadoop_client_hosts.run_sync,
                    "dpkg -l | awk '/ii.*+cdh/ {print $2\" \"}' > /root/cdh_package_list")

            self._remove_packages()

            confirm_on_failure(self.hadoop_client_hosts.run_async, 'apt-get update')

            confirm_on_failure(
                self.hadoop_client_hosts.run_sync, 'apt-cache policy hadoop | grep Candidate')
            ask_confirmation('Please verify that the candidate hadoop package is correct across all nodes.')

            self._install_packages_on_clients()

            logger.info('The procedure is completed.')
