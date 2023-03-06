"""Class based cookbook manage a useres u2f token"""
from logging import getLogger

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError


logger = getLogger(__name__)


class U2f(CookbookBase):
    """Manage U2f for a user.

    Usage example:
        cookbook sre.idm.u2f --enable $user
        cookbook sre.idm.u2f --disable $user
        cookbook sre.idm.u2f --disable --reset-token $user
        cookbook sre.idm.u2f --reset-token $user
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            '--reset-token',
            action='store_true',
            help="Reset the users token, forcing a re-registration",
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            '--enable', action='store_true', help='Enable u2f logins for the user'
        )
        group.add_argument(
            '--disable', action='store_true', help='disable u2f logins for the user'
        )
        parser.add_argument('username', required=True, help='the username to act on')
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return U2fRunner(args, self.spicerack)


class U2fRunner(CookbookRunnerBase):
    """U2f runner for specific user"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.mediawiki = spicerack.mediawiki()
        master_datacenter = self.mediawiki.get_master_datacenter()
        self.mwmaint = self.mediawiki.get_maintenance_host(master_datacenter)
        self.idp_host = spicerack.remote().query('A:idp and A:' + master_datacenter)
        self.modify_command = ''
        self.reset_command = None
        if args.enable:
            self.modify_command = f'/usr/local/bin/modify-mfa --enable {args.username}'
        elif args.disable:
            self.modify_command = f'/usr/local/bin/modify-mfa --disable {args.username}'
        if args.reset_token:
            self.reset_command = f'/usr/local/sbin/cas-remove-u2f --force {args.username}'
            self.message = f"reset-token {self.modify_command.split('--',1)[1]}"
        else:
            self.message = self.modify_command.split('--', 1)[1]

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.message

    def run(self):
        """Required by Spicerack API."""
        if self.modify_command is not None:
            try:
                self.mwmaint.run_sync(self.modify_command)
            except RemoteExecutionError as err:
                logger.error('%s: An error occured: %s', self.modify_command, err)
        if self.reset_command is not None:
            try:
                self.idp_host.run_sync(self.reset_command)
            except RemoteExecutionError as err:
                logger.error('%s: An error occured: %s', self.reset_command, err)
