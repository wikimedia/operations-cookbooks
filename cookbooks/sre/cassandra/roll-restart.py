"""Perform a rolling restart of some or all instances within a Cassandra cluster"""
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.cassandra import CASSANDRA_CLUSTERS

logger = logging.getLogger(__name__)


class RollRestartCassandra(CookbookBase):
    """Restart some or all Cassandra instances on a given cluster

    This cookbook checks that all Cassandra instances selected are reporting a status of up
    according to their systemd unit status. Once that check is complete it restarts all instances
    on each node sequentially.

    A specific set of nodes may be targeted by using the --query parameter with an optional cumin query string.
    If this parameter is omitted, then a cluster name must be provided and nodes within that cluster are restarted.

    Usage example:

    cookbook sre.cassandra.roll-restart --reason "Rolling AQS Cassndra cluster to pick up new Java runtime version" aqs
    cookbook sre.cassandra.roll-restart --query restbase1.eqiad.wmnet -r "Applying configuration change to restbase1"
    cookbook sre.cassandra.roll-restart --batch-sleep-seconds 600 --instance-sleep-seconds 30 -r "Type reason here" aqs
    """

    argument_reason_required = True

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('cluster', nargs='?',
                           choices=CASSANDRA_CLUSTERS,
                           help=('The name of the Cassandra cluster to work on. This refers to '
                                 'a Cumin alias. As an alternative, you can pass a specific Cumin '
                                 'host query using the --query argument'))
        group.add_argument('--query', help='A cumin query string')
        parser.add_argument('--batch-sleep-seconds', type=float, default=300.0,
                            help="Seconds to sleep between each host.")
        parser.add_argument('--instance-sleep-seconds', type=int, default=10,
                            help="Seconds to sleep between each Cassandra instance restart.")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartCassandraRunner(args, self.spicerack)


class RollRestartCassandraRunner(CookbookRunnerBase):
    """Cassandra Roll Restart cookbook runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner"""
        if args.cluster is not None:
            self.query = 'A:{}'.format(args.cluster)
        else:
            self.query = args.query
        ensure_shell_is_durable()

        self.cassandra_nodes = spicerack.remote().query(self.query)
        # In case we use a specific query to select nodes,
        # it is best to highlight the targets before proceeding.
        if args.cluster is None:
            ask_confirmation(
                f"Are the target nodes correct? {self.cassandra_nodes.hosts}"
            )
        self.puppet = spicerack.puppet(self.cassandra_nodes)
        self.reason = spicerack.admin_reason(reason=args.reason)
        self.alerting_hosts = spicerack.alerting_hosts(self.cassandra_nodes.hosts)
        self.reason = spicerack.admin_reason(args.reason)
        self.instance_sleep_seconds = args.instance_sleep_seconds
        self.batch_sleep_seconds = args.batch_sleep_seconds

        # perhaps we should create a c-foreach-status script?
        # See also https://phabricator.wikimedia.org/T229916
        status_cmd = """\
                STRING=''; \
                for i in $(c-ls) ; do STRING="${STRING} cassandra-${i}" ; done ; \
                systemctl status $STRING\
                """
        self.cassandra_nodes.run_sync(status_cmd)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for nodes matching {}: {}'.format(self.query, self.reason)

    def run(self):
        """Restart some or all Cassandra nodes on a given cluster"""
        # Puppet needs to be disabled on the target Cassandra hosts
        # to avoid any interference with c-foreach-restart
        with self.puppet.disabled(self.reason):
            logger.info('Checking that all Cassandra nodes are reported up by their systemd unit status.')
            with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=240)):
                self.cassandra_nodes.run_sync(
                    'c-foreach-restart -d ' + str(self.instance_sleep_seconds) + ' -a 20 -r 12',
                    batch_size=1,
                    batch_sleep=self.batch_sleep_seconds)

                logger.info('All Cassandra restarts completed!')
