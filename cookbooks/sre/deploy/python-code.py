"""Deploy a simple Python code software."""
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from wmflib.interactive import ask_confirmation, confirm_on_failure

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.deploy import DEPLOYMENT_CNAME


logger = logging.getLogger(__name__)


class Deploy(CookbookBase):
    """Deploy a simple Python code software with frozen wheels.

    It mimicks Scap for places where it's not yet available in scenarios like the ones described in:
    https://phabricator.wikimedia.org/T180023
    The deploy is done sequentially, one host at a time, pausing and asking the user what to do on error.

    Usage example:
      cookbook sre.deploy.python-code -r 'some reason' 'someproject' 'A:somealias'
      cookbook sre.deploy.python-code -r 'some reason' -t T12345 'someproject' 'somehost.example.com'

    """

    owner_team = "Infrastructure Foundations"

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('project', help='The name of the project on the deployment server.')
        parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
        parser.add_argument('-r', '--reason', required=True,
                            help=('The reason for the downtime. The current username and originating host are '
                                  'automatically added.'))
        parser.add_argument('-t', '--task-id',
                            help='An optional task ID to refer in the downtime message (i.e. T12345).')
        parser.add_argument('-u', '--user',
                            help=('By default the deployment will be run with the deploy-$project user. Use this '
                                  'parameter to override it in case a different one should be used.'))

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return DeployRunner(args, self.spicerack)


class DeployRunner(CookbookRunnerBase):
    """Deploy Python code runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        remote = spicerack.remote()
        self.remote_hosts = remote.query(args.query)
        if not self.remote_hosts.hosts:
            raise RuntimeError(f'No host found for query "{args.query}"')

        self.project = args.project
        self.task_id = args.task_id
        self.deployment_host = remote.query(spicerack.dns().resolve_cname(DEPLOYMENT_CNAME))
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

        if args.user:
            if self.project not in args.user:
                ask_confirmation(f'The -u/--user provided "{args.user}" does not seem related to the project name '
                                 f'"{self.project}". Are you sure you want to continue using the user "{args.user}"?')
            self.user = args.user
        else:
            self.user = f'deploy-{self.project}'

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

        if len(self.remote_hosts) <= 5:
            hosts_message = str(self.remote_hosts)
        else:
            hosts_message = f'{len(self.remote_hosts)} hosts matching {args.query}'

        self.message = f'{self.project} to {hosts_message} with reason: {self.reason}'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.message

    @property
    def lock_args(self):
        """Make the cookbook lock exclusive per-project."""
        return LockArgs(suffix=self.project, concurrency=1, ttl=1800)

    def run(self):
        """Required by Spicerack API."""
        self.deployment_host.run_sync(
            f'runuser -u mwdeploy -- /usr/bin/git -C "/srv/deployment/{self.project}/deploy" update-server-info')
        for remote_host in self.remote_hosts.split(len(self.remote_hosts)):  # Do one host at a time
            confirm_on_failure(remote_host.run_sync,
                               f'runuser -u {self.user} -- /usr/local/bin/python-deploy-venv {self.project}')

        if self.phabricator is not None:
            self.phabricator.task_comment(self.task_id, f'Deployed {self.message}')
