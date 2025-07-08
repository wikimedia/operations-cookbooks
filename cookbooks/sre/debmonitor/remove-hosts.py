"""Remove host(s) from DebMonitor"""
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import NodeSet, RemoteError

from wmflib.interactive import confirm_on_failure, ask_confirmation
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class RemoveHosts(CookbookBase):
    """Remove host(s) from DebMonitor

    Usage example:
        cookbook sre.debmonitor.remove-hosts -t T123456 example1001.eqiad.wmnet

    """

    argument_task_required = False

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('query', help='Cumin query to match the host(s) to remove from DebMonitor')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RemoveHostsRunner(args, self.spicerack)


class RemoveHostsRunner(CookbookRunnerBase):
    """Debmonitor host removal cookbook runner class."""

    skip_start_sal = True  # Only runs a few seconds anyway, avoid logging START

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.debmonitor = spicerack.debmonitor()
        self.removed_hosts = 0
        self.username = spicerack.username
        try:
            self.hosts = spicerack.remote().query(args.query).hosts
        except RemoteError:
            query_hosts = NodeSet(args.query)
            ask_confirmation(
                'Your query did not match any hosts. This can happen if the host\n'
                'record was already removed from Puppetdb, but persists in\n'
                'DebMonitor. Do you want to proceed? The following {l} hosts will be\n'
                'affected: {query_hosts}\n'.
                format(l=len(query_hosts), query_hosts=query_hosts))
            self.hosts = query_hosts

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.task_id = args.task_id

        self.log_message = 'for {n} hosts: {hosts}'.format(n=len(self.hosts), hosts=self.hosts)

    @property
    def runtime_description(self):
        """Return the status message for the cookbook."""
        return self.log_message

    def run(self):
        """Required by Spicerack API."""
        logging.info('Removing %s from Debmonitor', self.hosts)
        for fqdn in self.hosts:
            confirm_on_failure(self.debmonitor.host_delete, fqdn)

        phab_log = "Cookbook {name} run by {user}: {msg}".format(
            name=__name__, user=self.username, msg=self.log_message)

        self.phabricator.task_comment(self.task_id, phab_log)
