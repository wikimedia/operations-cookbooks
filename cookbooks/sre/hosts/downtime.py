"""Downtime hosts and all their services in Icinga."""
import argparse
import logging

from datetime import timedelta

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

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        if not any((args.minutes, args.hours, args.days)):
            logger.info('No downtime length option specified, using default value of %d hours',
                        Downtime.DEFAULT_DOWNTIME_HOURS)
            args.hours = Downtime.DEFAULT_DOWNTIME_HOURS

        return DowntimeRunner(args, self.spicerack)


class DowntimeRunner(CookbookRunnerBase):
    """Donwtime cookbook runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.duration = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
        self.hosts = spicerack.remote().query(args.query).hosts
        if not self.hosts:
            raise RuntimeError('No host found for query "{query}"'.format(query=args.query))

        self.task_id = args.task_id
        self.icinga_hosts = spicerack.icinga_hosts(self.hosts)
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
            hosts_message = '{num} hosts'.format(num=len(self.hosts))

        self.short_message = ('for {duration} on {hosts_message} with reason: {reason}').format(
            duration=self.duration, hosts_message=hosts_message, reason=args.reason)

        self.long_message = ('Icinga downtime set by {owner} for {s.duration} {num} host(s) and their services '
                             'with reason: {reason}\n```\n{s.hosts}\n```').format(
                                 owner=self.reason.owner, s=self, num=len(self.hosts), reason=args.reason)

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
