"""Downtime a single host and reboot it"""
import logging
import time

from datetime import datetime, timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.icinga import IcingaError
from spicerack.puppet import PuppetHostsCheckError
from wmflib.interactive import ask_input
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class RebootSingleHost(CookbookBase):
    """Downtime a single host and reboot it

    - Set Icinga/Alertmanager downtime
    - Reboot
    - Wait for host to come back online
    - Remove the Icinga/Alertmanager downtime after the host has been rebooted and the
      first Puppet run is complete

    This is meant for one off servers and doesn't support pooling/depooling
    clustered services (yet).

    Usage example:
        cookbook sre.hosts.reboot-single sretest1001.eqiad.wmnet

    """

    owner_team = 'Infrastructure Foundations'
    argument_reason_required = False
    argument_task_required = False

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootSingleHostRunner(args, self.spicerack)

    def argument_parser(self):
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument('host', help='A single host to be rebooted (specified in Cumin query syntax)')
        parser.add_argument('--depool', help='Whether to run depool/pool on the server around reboots.',
                            action='store_true')
        parser.add_argument('--enable-puppet', help='Enable Puppet with a specific reason.')
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

        self.depool = False
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.icinga_hosts = spicerack.icinga_hosts(self.remote_host.hosts)
        self.puppet = spicerack.puppet(self.remote_host)
        self.reason = spicerack.admin_reason('Rebooting host' if not args.reason else args.reason)

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            self.message = (
                "Host {host} rebooted by {owner} with reason: {reason}\n"
            ).format(host=args.host, owner=self.reason.owner, reason=args.reason)
        else:
            self.phabricator = None

        if args.enable_puppet is not None:
            # try to enable puppet before we check if its disabled
            self.puppet.enable(spicerack.admin_reason(args.enable_puppet), verbatim_reason=True)

        try:
            self.puppet.check_enabled()
            self.puppet_enabled = True
            self.depool = args.depool
        except PuppetHostsCheckError as error:
            self.puppet_enabled = False
            logger.warning("Puppet is disabled we will not wait for a puppet run or monitoring: %s", error)
            if args.depool:
                answer = ask_input(
                    "Puppet is disabled are you sure you want to manage the pool state",
                    ("yes", "no")
                )
                if answer == 'yes':
                    self.depool = True

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for host {}'.format(self.remote_host.hosts)

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.remote_host.hosts).split('.', 1)[0], concurrency=1, ttl=600)

    def run(self):
        """Reboot the host"""
        ret = 0
        with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
            if self.phabricator is not None:
                self.phabricator.task_comment(self.task_id, self.message)

            if self.depool:
                self.remote_host.run_async('depool')
                logger.info('Waiting a 30 second grace period after depooling')
                time.sleep(30)
            reboot_time = datetime.utcnow()
            self.remote_host.reboot()
            self.remote_host.wait_reboot_since(reboot_time, print_progress_bars=False)

            if self.puppet_enabled:
                self.puppet.wait_since(reboot_time)
                try:
                    self.icinga_hosts.wait_for_optimal(skip_acked=True)
                except IcingaError:
                    ret = 1
                    logger.error(
                        "The host's status is not optimal according to Icinga, "
                        "please check it.")

            if self.depool:
                if ret == 0:
                    self.remote_host.run_async('pool')
                else:
                    logger.warning(
                        "NOT repooling the services due to the host's Icinga status.")

        return ret
