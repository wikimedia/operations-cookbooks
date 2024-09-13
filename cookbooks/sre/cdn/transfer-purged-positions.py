"""Transfer kafka consumer offsets for purged topics.

This cookbook is useful when moving purged from one DC to another

Those are the steps provided by the cookbook:

- Depool the host (automatic)
- Disable icinga notification (automatic)
- Check that puppet-agent is disabled (pre_scripts)
- Transfer kafka consumer offset (_custom_action)
- Enable and run Puppet (puppet agent **must** be disabled first with cumin) (_custom_action())
- Repool the host (automatic)
"""

from argparse import Namespace

from wmflib.constants import ALL_DATACENTERS, CORE_DATACENTERS
from wmflib.interactive import confirm_on_failure

from spicerack import Spicerack
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class TransferPurgedOffsets(SREBatchBase):
    r"""Roll apply configuration using puppet-agent for purged

    This cookbook is useful when moving purged consumers from one kafka DC to another
    and syncing positions across consumers groups is required

    Example usage(s):

        cookbook sre.cdn.transfer-purged-positions \
            --alias cp-text_codfw \
            --dc-to codfw \
            --reason 'move purged back to codfw' \
            --puppet-reason 'TXXXXXX'
    """

    grace_sleep = 30  # Wait 30s between batches
    batch_max = 1
    valid_actions = ('custom',)
    max_failed = 1

    def argument_parser(self):
        """Argument parser"""
        parser = super().argument_parser()
        parser.add_argument(
            '--puppet-reason',
            type=str,
            required=True,
            help="Puppet reason. Must match puppet disable reason used by Cumin"
        )
        parser.add_argument(
            "--dc-to",
            type=str,
            required=True,
            choices=CORE_DATACENTERS,
            help="Name of the datacenter switch to. One of %(choices)s.",
        )

        return parser

    # required
    def get_runner(self, args):
        """As specified by the Spicerack API."""
        return TransferPurgedOffsetsRunner(args, self.spicerack)


class TransferPurgedOffsetsRunner(SRELBBatchRunnerBase):
    r"""Roll apply configuration using puppet-agent."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """We need to override this in order to use the args.puppet_reason"""
        super().__init__(args, spicerack)
        self._puppet_reason = spicerack.admin_reason(reason=args.puppet_reason)
        self._dc_to = args.dc_to
        spicerack_remote = spicerack.remote()
        kafka_hosts = spicerack_remote.query(f"A:kafka-main-{self._dc_to}")
        # use a single host from the kafka-main cluster
        self.kafka_host = spicerack_remote.query(kafka_hosts.hosts[0])

    @property
    def allowed_aliases(self):
        """Required"""
        aliases = []
        for role in ('text', 'upload'):
            for dc in ALL_DATACENTERS:
                aliases.append(f'cp-{role}_{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases

    def _custom_action(self, hosts, _):
        """The actual transfer consumer position / run puppet logic resides here."""
        # we are safe here cause batch_max = 1
        hostname = hosts.hosts[0].split('.')[0]
        cmd = f"kafka-consumer-groups --bootstrap-server localhost:9092 --group {hostname} --reset-offsets " \
            "--to-latest --all-topics --execute"
        confirm_on_failure(self.kafka_host.run_sync, cmd)

        puppet = self._spicerack.puppet(hosts)
        confirm_on_failure(puppet.run, enable_reason=self._puppet_reason)

    @property
    def pre_scripts(self):
        """We check that puppet agent is NOT enabled and fail in case by running shell command on the host."""
        return [
            '! puppet-enabled'
        ]
