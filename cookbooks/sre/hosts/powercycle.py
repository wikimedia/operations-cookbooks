"""Powercycle a single host."""
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.redfish import ChassisResetPolicy
from wmflib.interactive import confirm_on_failure
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import reboot_chassis

logger = logging.getLogger(__name__)


class PowercycleHost(CookbookBase):
    """Powercycle a single host via Redfish and wait for it to be up (BMC perspective).

    Usage example:
        cookbook sre.hosts.powercycle sretest1001

    """

    owner_team = 'Infrastructure Foundations'
    argument_reason_required = False
    argument_task_required = False

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return PowercycleHostRunner(args, self.spicerack)

    def argument_parser(self):
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument('host', help='A single hostname to be powercycled.')
        return parser


class PowercycleHostRunner(CookbookRunnerBase):
    """Powercycle a single host runner."""

    def __init__(self, args, spicerack):
        """Powercycle a single host."""
        if '.' in args.host:
            raise RuntimeError("Please use a hostname, not a fqdn.")
        self.reason = spicerack.admin_reason('Powercycling host' if not args.reason else args.reason)

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.task_id = args.task_id
        self.message = (
            "Host {host} powercycled by {owner} with reason: {reason}\n"
        ).format(host=args.host, owner=self.reason.owner, reason=args.reason)

        self.redfish = spicerack.redfish(args.host)
        netbox_server = spicerack.netbox_server(args.host)
        netbox_data = netbox_server.as_dict()
        self.vendor = netbox_data['device_type']['manufacturer']['slug']
        self.host = args.host

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for host {}'.format(self.host)

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.host), concurrency=1, ttl=600)

    def run(self):
        """Powercycle the host"""
        self.phabricator.task_comment(self.task_id, self.message)

        logger.info("Testing connection to the BMC via Redfish.")
        confirm_on_failure(self.redfish.check_connection)
        reboot_chassis(
            self.vendor, self.redfish, chassis_reset_policy=ChassisResetPolicy.FORCE_RESTART)
