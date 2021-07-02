"""Downtime a single host and reboot it"""

import argparse
import logging
import time

from datetime import datetime, timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.icinga import IcingaError
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class RebootSingleHost(CookbookBase):
    """Downtime a single host and reboot it

    - Set Icinga downtime
    - Reboot
    - Wait for host to come back online
    - Remove the Icinga downtime after the host has been rebooted and the
      first Puppet run is complete

    This is meant for one off servers and doesn't support pooling/depooling
    clustered services (yet).

    Usage example:
        cookbook sre.hosts.reboot-single sretest1001.eqiad.wmnet

    """

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootSingleHostRunner(args, self.spicerack)

    def argument_parser(self):
        """Parse arguments"""
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('host', help='A single host to be rebooted (specified in Cumin query syntax)')
        parser.add_argument('-r', '--reason', required=False,
                            help=('The reason for the reboot. The current username and originating host are '
                                  'automatically added.'))
        parser.add_argument('-t', '--task-id',
                            help='An optional task ID to refer in the downtime message (i.e. T12345).')
        parser.add_argument('--depool', help='Whether to run depool/pool on the server around reboots.',
                            action='store_true')
        return parser


class RebootSingleHostRunner(CookbookRunnerBase):
    """Downtime a single host and reboot it runner."""

    def __init__(self, args, spicerack):
        """Downtime a single host and reboot it"""
        self.remote_host = spicerack.remote().query(args.host)

        if len(self.remote_host) == 0:
            raise RuntimeError('Specified server not found, bailing out')

        if len(self.remote_host) != 1:
            raise RuntimeError('Only a single server can be rebooted')

        self.icinga_hosts = spicerack.icinga_hosts(self.remote_host.hosts)
        self.puppet = spicerack.puppet(self.remote_host)
        self.reason = spicerack.admin_reason('Rebooting host' if not args.reason else args.reason)

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            self.message = ('Host rebooted by {owner} with reason: {reason}\n').format(
                owner=self.reason.owner, reason=args.reason)
        else:
            self.phabricator = None

        self.depool = args.depool

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for host {}'.format(self.remote_host.hosts)

    def run(self):
        """Reboot the host"""
        with self.icinga_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
            if self.phabricator is not None:
                self.phabricator.task_comment(self.task_id, self.message)

            if self.depool:
                self.remote_host.run_async('depool')
                logger.info('Waiting a 30 second grace period after depooling')
                time.sleep(30)
            reboot_time = datetime.utcnow()
            self.remote_host.reboot()
            self.remote_host.wait_reboot_since(reboot_time)
            self.puppet.wait_since(reboot_time)

            # First let's try to check if icinga is already in optimal state.
            # If not, we require a recheck all service, then
            # wait a grace period before declaring defeat.
            icinga_ok = self.icinga_hosts.get_status().optimal
            if not icinga_ok:
                self.icinga_hosts.recheck_all_services()
                try:
                    self.icinga_hosts.wait_for_optimal()
                    icinga_ok = True
                except IcingaError:
                    logger.error(
                        "The host's status is not optimal according to Icinga, "
                        "please check it.")

            if self.depool:
                if icinga_ok:
                    self.remote_host.run_async('pool')
                else:
                    logger.warning(
                        "NOT repooling the services due to the host's Icinga status.")
