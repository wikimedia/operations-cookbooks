"""WMCS Openstack - Show the current cloudnets and some info.

Usage example:
    cookbook wmcs.openstack.cloudnet.show \
    --controlling-node-fqdn cloudcontrol1005.wikimedia.org

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.lib.openstack import OpenstackAPI
from cookbooks.wmcs.lib.openstack.neutron import NeutronAgentType, NeutronController

LOGGER = logging.getLogger(__name__)


class Show(CookbookBase):
    """WMCS Openstack cookbook to show the current status of the neutron setup."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--controlling-node-fqdn",
            required=False,
            default="cloudcontrol1003.wikimedia.org",
            help="FQDN of one of the nodes to manage the cluster.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ShowRunner(
            controlling_node_fqdn=args.controlling_node_fqdn,
            spicerack=self.spicerack,
        )


class ShowRunner(CookbookRunnerBase):
    """Runner for Show"""

    def __init__(
        self,
        controlling_node_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.controlling_node_fqdn = controlling_node_fqdn
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn=self.controlling_node_fqdn,
            project="admin",
        )
        self.neutron_controller = NeutronController(openstack_api=self.openstack_api)

    def run(self) -> None:
        """Main entry point"""
        all_agents = self.neutron_controller.agent_list()
        l3_agents = [str(agent) for agent in all_agents if agent.agent_type == NeutronAgentType.L3_AGENT]
        dhcp_agents = [str(agent) for agent in all_agents if agent.agent_type == NeutronAgentType.DHCP_AGENT]
        metadata_agents = [str(agent) for agent in all_agents if agent.agent_type == NeutronAgentType.METADATA_AGENT]
        linux_bridge_agents = [
            str(agent) for agent in all_agents if agent.agent_type == NeutronAgentType.LINUX_BRIDGE_AGENT
        ]
        cloudnets = self.neutron_controller.get_cloudnets()

        LOGGER.info("Got L3 Agents:\n    %s", "\n    ".join(l3_agents))
        LOGGER.info("Got dhcp Agents:\n    %s", "\n    ".join(dhcp_agents))
        LOGGER.info("Got metadata Agents:\n    %s", "\n    ".join(metadata_agents))
        LOGGER.info("Got linux bridge Agents:\n    %s", "\n    ".join(linux_bridge_agents))
        LOGGER.info("Got cloudnets (should be the same as L3 agents):\n    %s", "\n    ".join(cloudnets))
