"""Class based cookbook to Logout a user from some or all services on a set of hosts using the logoutd tools."""
from argparse import ArgumentParser
from logging import getLogger

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, confirm_on_failure

from cookbooks import ArgparseFormatter


logger = getLogger(__name__)


class Logout(CookbookBase):
    """Logout a user from some or all services on a set of hosts using the logoutd tools.

    Usage example:
        cookbook sre.idm.logout --uid $user_id --cn $common_name 'A:all'
        cookbook sre.idm.logout --uid $user_id --cn $common_name 'A:idp'
        cookbook sre.idm.logout --uid $user_id --cn $common_name 'A:idp' service1 service2 service3
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = ArgumentParser(
            description=self.__doc__, formatter_class=ArgparseFormatter
        )
        parser.add_argument(
            'query', help='Cumin query to match the host(s) to act upon.'
        )
        # TODO: We should only need to require one of theses and then use ldap to get the other
        parser.add_argument('-u', '--uid', help='The uid to act upon', required=True)
        parser.add_argument('-c', '--cn', help='The cn to act upon', required=True)
        parser.add_argument(
            'services',
            nargs='*',
            help=('An optional list of services to log the user out of, '
                  'default behaviour is to log the user out of all services'),
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return LogoutRunner(args, self.spicerack)


class LogoutRunner(CookbookRunnerBase):
    """Logout specific user"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        remote = spicerack.remote()
        self.remote_hosts = remote.query(args.query)
        if not self.remote_hosts.hosts:
            raise RuntimeError(f'No host found for query "{args.query}"')

        if args.services:
            services_args = f'-S {"-S ".join(args.services)}'
            if len(args.services) > 3:
                services_message = f'{len(args.services)} services'
            else:
                services_message = ' '.join(args.services)
        else:
            services_message = 'all services'
            services_args = ''

        hosts_message = f'{len(self.remote_hosts)} hosts'
        self.message = f'Logging {args.cn} out of {services_message} on: {hosts_message}'
        self.command = f'/usr/local/sbin/wmf-run-logout-scripts {services_args} logout --uid {args.uid} --cn {args.cn}'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.message

    def run(self):
        """Required by Spicerack API."""
        ask_confirmation(self.message)
        confirm_on_failure(self.remote_hosts.run_sync, self.command)
