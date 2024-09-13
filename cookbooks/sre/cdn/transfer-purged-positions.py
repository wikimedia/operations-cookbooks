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
from spicerack.kafka import ConsumerDefinition
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class TransferPurgedOffsets(SREBatchBase):
    r"""Roll apply configuration using puppet-agent for purged

    This cookbook is useful when moving purged consumers from one kafka DC to another
    and syncing positions across consumers groups is required

    Example usage(s):

        cookbook sre.cdn.transfer-purged-positions \
            --alias cp-text_codfw \
            --dc-from eqiad \
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
            "--dc-from",
            type=str,
            required=True,
            choices=CORE_DATACENTERS,
            help="Name of the current datacenter used by the consumers. One of %(choices)s.",
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
        self._dc_from = args.dc_from
        self._dc_to = args.dc_to

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
        src = ConsumerDefinition(self._dc_from, 'main', hostname)
        dst = ConsumerDefinition(self._dc_to, 'main', hostname)
        kafka = self._spicerack.kafka()
        kafka.transfer_consumer_position(["resource-purge"], src, dst)

        puppet = self._spicerack.puppet(hosts)
        confirm_on_failure(puppet.run, enable_reason=self._puppet_reason)

    @property
    def pre_scripts(self):
        """We check that puppet agent is NOT enabled and fail in case by running shell command on the host."""
        return [
            '! puppet-enabled'
        ]
