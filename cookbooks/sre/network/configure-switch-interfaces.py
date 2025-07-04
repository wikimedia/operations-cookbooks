"""Configure the switch interfaces of a given host"""
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable


from cookbooks.sre.network import configure_switch_interfaces, run_homer

logger = logging.getLogger(__name__)


class ConfigSwitchInterfaces(CookbookBase):
    """Configure the switch interfaces of a given host

    Short standalone cookbook to:
    1/ Test the various helper functions
    2/ Configure only the network side if other cookbooks failed
    their network part

    Script is idempotent and prompts the user before any intrusive change.

    Usage example:
        cookbook sre.network.configure-switch-interfaces netmon1002

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('host', help='Physical server hostname (not FQDN)')
        parser.add_argument('--homer', action='store_true', help='Use Homer to configure the switches')
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ConfigSwitchInterfacesRunner(args, self.spicerack)


class ConfigSwitchInterfacesRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the switch config interface runner."""
        ensure_shell_is_durable()
        self.netbox = spicerack.netbox()
        self.verbose = spicerack.verbose
        self.netbox_server = spicerack.netbox_server(args.host)
        self.netbox_data = self.netbox_server.as_dict()
        self.remote = spicerack.remote()
        self.dry_run = spicerack.dry_run
        self.args = args

        if self.netbox_data['is_virtual']:
            logger.error("This cookbook is intended for baremetal hosts only")

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"for host {self.netbox_data['name']}"

    def run(self):
        """Required by Spicerack API."""
        if self.args.homer:
            # TODO: doesn't work for virtual-chassis
            run_homer(queries=[f'{hostname}.*' for hostname in self.netbox_server.switches], dry_run=self.dry_run)
        else:
            configure_switch_interfaces(self.remote, self.netbox, self.netbox_data, self.verbose)
