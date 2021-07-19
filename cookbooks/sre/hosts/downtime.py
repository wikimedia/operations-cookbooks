"""Downtime hosts and all their services in Icinga."""
import argparse
import logging

from datetime import timedelta

from cumin import NodeSet
from wmflib.interactive import ask_confirmation

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


logger = logging.getLogger(__name__)


class Downtime(CookbookBase):
    """Downtime hosts and all their services in Icinga.

    - Optionally force a Puppet run on the Icinga host to pick up new hosts or services
    - Set Icinga downtime for the given time with a default of 4h if not specified

    Usage example:
      cookbook sre.hosts.downtime --days 5 -r 'some reason' 'somehost1001*'
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' somehost1001.eqiad.wmnet
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' 'O:some::role'
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' --force 'somehost100[1-5].mgmt,192.168.1.1'

    """

    DEFAULT_DOWNTIME_HOURS = 4

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
        parser.add_argument('-r', '--reason', required=True,
                            help=('The reason for the downtime. The current username and originating host are '
                                  'automatically added.'))
        parser.add_argument('-t', '--task-id',
                            help='An optional task ID to refer in the downtime message (i.e. T12345).')
        parser.add_argument('-M', '--minutes', type=int, default=0,
                            help='For how many minutes the downtime should last. [optional, default=0]')
        parser.add_argument('-H', '--hours', type=int, default=0,
                            help='For how many hours the downtime should last. [optional, default=0]')
        parser.add_argument('-D', '--days', type=int, default=0,
                            help='For how many days the downtime should last. [optional, default=0]')
        parser.add_argument('--force-puppet', action='store_true',
                            help='Force a Puppet run on the Icinga host to pick up new hosts or services.')
        parser.add_argument('--force', action='store_true',
                            help=('Override the check that use a Cumin query to validate the given hosts. Useful when '
                                  'you want to remove a downtime from a Icinga "host" that is not a real host or '
                                  'not anymore queryable via Cumin.'))

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        if not any((args.minutes, args.hours, args.days)):
            logger.info('No downtime length option specified, using default value of %d hours',
                        Downtime.DEFAULT_DOWNTIME_HOURS)
            args.hours = Downtime.DEFAULT_DOWNTIME_HOURS

        return DowntimeRunner(args, self.spicerack)


class DowntimeRunner(CookbookRunnerBase):
    """Downtime cookbook runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.duration = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
        if args.force:
            self.hosts = NodeSet(args.query)
            ask_confirmation(f'Will remove downtime for {len(self.hosts)} unverified hosts: {self.hosts}')
        else:
            self.hosts = spicerack.remote().query(args.query).hosts
            if not self.hosts:
                raise RuntimeError(f'No host found for query "{args.query}". Use --force targeting Icinga hosts that '
                                   'are not real hosts.')

        self.task_id = args.task_id
        self.icinga_hosts = spicerack.icinga_hosts(self.hosts, verbatim_hosts=args.force)
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

        if args.force_puppet:
            self.puppet = spicerack.puppet(spicerack.icinga_master_host)
        else:
            self.puppet = None

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

        if len(self.hosts) <= 5:
            hosts_message = str(self.hosts)
        else:
            hosts_message = f'{len(self.hosts)} hosts'

        self.short_message = f'for {self.duration} on {hosts_message} with reason: {args.reason}'

        self.long_message = (f'Icinga downtime set by {self.reason.owner} for {self.duration} {len(self.hosts)} '
                             f'host(s) and their services with reason: {args.reason}\n```\n{self.hosts}\n```')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.short_message

    def run(self):
        """Required by Spicerack API."""
        if self.puppet is not None:
            logging.info('Forcing a Puppet run on the Icinga server')
            self.puppet.run(quiet=True, attempts=30)

        logging.info('Downtiming %s', self.runtime_description)
        self.icinga_hosts.downtime(self.reason, duration=self.duration)

        if self.phabricator is not None:
            self.phabricator.task_comment(self.task_id, self.long_message)
