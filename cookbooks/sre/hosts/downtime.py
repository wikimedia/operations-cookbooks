"""Downtime hosts and all their services in Icinga and Alertmanager."""
import logging

from argparse import ArgumentParser
from datetime import timedelta

from cumin import NodeSet
from wmflib.decorators import retry
from wmflib.interactive import ask_confirmation

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.icinga import IcingaStatusNotFoundError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


logger = logging.getLogger(__name__)


def enrich_argument_parser_with_downtime_duration(parser: ArgumentParser) -> ArgumentParser:
    """Add downtime duration arguments (--minutes, --hours, --days) to the provided parser."""
    parser.add_argument('-M', '--minutes', type=int, default=0,
                        help='For how many minutes the downtime should last. [optional, default=0]')
    parser.add_argument('-H', '--hours', type=int, default=0,
                        help='For how many hours the downtime should last. [optional, default=0]')
    parser.add_argument('-D', '--days', type=int, default=0,
                        help='For how many days the downtime should last. [optional, default=0]')
    return parser


class Downtime(CookbookBase):
    """Downtime hosts and all their services in Icinga and Alertmanager.

    - Optionally force a Puppet run on the Icinga host to pick up new hosts or services
    - Set an Icinga downtime for the given time with a default of 4h if not specified
    - Set an Alertmanager silence for the given time with a default of 4h if not specified

    Usage example:
      cookbook sre.hosts.downtime --days 5 -r 'some reason' 'somehost1001*'
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' somehost1001.eqiad.wmnet
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' 'O:some::role'
      cookbook sre.hosts.downtime --minutes 20 -r 'some reason' --force 'somehost100[1-5].mgmt,192.168.1.1'

    """

    owner_team = "Observability"
    argument_reason_required = True
    argument_task_required = False
    DEFAULT_DOWNTIME_HOURS = 4

    def argument_parser(self) -> ArgumentParser:
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser = enrich_argument_parser_with_downtime_duration(parser)
        parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
        parser.add_argument('--force-puppet', action='store_true',
                            help='Force a Puppet run on the Icinga host to pick up new hosts or services.')
        parser.add_argument('--force', action='store_true',
                            help=('Override the check that use a Cumin query to validate the given hosts. Useful when '
                                  'you want to downtime a "host" that is not a real host like a service or '
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
            ask_confirmation(f'Will downtime {len(self.hosts)} unverified hosts: {self.hosts}')
        else:
            self.hosts = spicerack.remote().query(args.query).hosts
            if not self.hosts:
                raise RuntimeError(f'No host found for query "{args.query}". Use --force targeting Icinga/Alertmanager '
                                   'hosts that are not real hosts.')

        self.task_id = args.task_id
        self.icinga_hosts = spicerack.icinga_hosts(self.hosts, verbatim_hosts=args.force)
        self.alerting_hosts = spicerack.alerting_hosts(self.hosts, verbatim_hosts=args.force)
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
        self.lock = spicerack.lock

        if args.force_puppet:
            self.puppet = spicerack.puppet(spicerack.icinga_master_host())
        else:
            self.puppet = None

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        if len(self.hosts) <= 5:
            hosts_message = str(self.hosts)
        else:
            hosts_message = f'{len(self.hosts)} hosts'

        self.short_message = f'for {self.duration} on {hosts_message} with reason: {args.reason}'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.short_message

    @property
    def skip_start_sal(self):
        """Skip the START SAL logging in all cases except when a making a puppet run on the Icinga host (slow run)."""
        return self.puppet is None

    def run(self):
        """Required by Spicerack API."""
        if self.puppet is not None:
            logging.info('Forcing a Puppet run on the Icinga server')
            # lock to work around T355187
            with self.lock().acquired('sre.hosts.downtime:force-puppet', concurrency=1, ttl=600):
                self.puppet.run(quiet=True, attempts=60, timeout=600)
            logging.info('Polling Icinga status to wait for all hosts to be known to Icinga')
            self._poll_status()

        logging.info('Downtiming %s', self.runtime_description)
        downtime_id = self.alerting_hosts.downtime(self.reason, duration=self.duration)

        message = (f'Icinga downtime and Alertmanager silence (ID={downtime_id}) set by {self.reason.owner} '
                   f'for {self.duration} on {len(self.hosts)} host(s) and their services with reason: '
                   f'{self.reason.reason}\n```\n{self.hosts}\n```')

        self.phabricator.task_comment(self.task_id, message, raises=False)

    @retry(  # pylint: disable=no-value-for-parameter
        tries=10,
        delay=timedelta(seconds=10),
        backoff_mode='constant',
        failure_message='Missing hosts from Icinga status, keep polling',
        exceptions=(IcingaStatusNotFoundError,),
    )
    def _poll_status(self):
        """Poll Icinga until all the hosts are known, raises IcingaStatusNotFoundError if any is missing."""
        self.icinga_hosts.get_status()
