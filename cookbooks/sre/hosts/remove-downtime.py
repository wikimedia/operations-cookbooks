"""Remove hosts and all their services downtimes in Icinga."""
import argparse
import logging

from cumin import NodeSet
from wmflib.interactive import ask_confirmation

from spicerack.cookbook import CookbookBase, CookbookRunnerBase


logger = logging.getLogger(__name__)


class RemoveDowntime(CookbookBase):
    """Remove the Icinga downtime for the selected hosts with all their services.

    In case Icinga hosts that are not real hosts should be targeted, --force can be used and the hosts can be
    passed with the usual ClusterShell's NodeSet syntax (the same of Cumin).

    Usage example:
      cookbook sre.hosts.remove-downtime 'somehost1001*'
      cookbook sre.hosts.remove-downtime 'A:some-cumin-alias'
      cookbook sre.hosts.remove-downtime --force 'somehost100[1-2].mgmt,192.168.1.1'

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('query', help=('Cumin query to match the host(s) to act upon. If --force is set they will '
                                           'be used verbatim even if not mathing any host from a Cumin query.'))
        parser.add_argument('--force', action='store_true',
                            help=('Override the check that use a Cumin query to validate the given hosts. Useful when '
                                  'you want to remove a donwtime from a Icinga "host" that is not a real host or '
                                  'not anymore queryable via Cumin.'))

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RemoveDowntimeRunner(args, self.spicerack)


class RemoveDowntimeRunner(CookbookRunnerBase):
    """Remove donwtime cookbook runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        if args.force:
            self.hosts = NodeSet(args.query)
            ask_confirmation(f'Will remove downtime for {len(self.hosts)} unverified hosts: {self.hosts}')
        else:
            self.hosts = spicerack.remote().query(args.query).hosts
            if not self.hosts:
                raise RuntimeError(f'No host found for query "{args.query}". Use --force targeting Icinga hosts that '
                                   'are not real hosts.')

        self.icinga_hosts = spicerack.icinga_hosts(self.hosts, verbatim_hosts=args.force)

        if len(self.hosts) <= 5:
            self.hosts_message = str(self.hosts)
        else:
            self.hosts_message = f'{len(self.hosts)} hosts'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return f'for {self.hosts_message}'

    def run(self):
        """Required by Spicerack API."""
        logging.info('Removing downtime for %s', self.hosts)
        self.icinga_hosts.remove_downtime()
