"""WMCS Openstack - Show the current cloudnets and some info.

Usage example:
    cookbook wmcs.openstack.cloudnet.show \
        --deployment eqiad1

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.openstack.common import Deployment, OpenstackAPI, get_control_nodes
from cookbooks.wmcs.libs.openstack.neutron import NeutronAgentType, NeutronController

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
            "--deployment",
            required=True,
            default=Deployment.EQIAD1,
            choices=list(Deployment),
            type=Deployment,
            help="Site to get the info for",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ShowRunner(
            deployment=args.deployment,
            spicerack=self.spicerack,
        )


class ShowRunner(CookbookRunnerBase):
    """Runner for Show"""

    def __init__(
        self,
        deployment: Deployment,
        spicerack: Spicerack,
    ):
        """Init"""
        self.controlling_node_fqdn = get_control_nodes(deployment=deployment)[0]
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
        routers = self.neutron_controller.router_list()
        routers_str = ""
        for router in routers:
            agents_on_router = self.neutron_controller.list_agents_hosting_router(router=router.router_id)
            routers_str += f"{router}\n        "
            routers_str += "\n        ".join(str(agent) for agent in agents_on_router)

        LOGGER.info("Got Routers:\n    %s", routers_str)
        LOGGER.info("Got L3 Agents:\n    %s", "\n    ".join(l3_agents))
        LOGGER.info("Got dhcp Agents:\n    %s", "\n    ".join(dhcp_agents))
        LOGGER.info("Got metadata Agents:\n    %s", "\n    ".join(metadata_agents))
        LOGGER.info("Got linux bridge Agents:\n    %s", "\n    ".join(linux_bridge_agents))
        LOGGER.info("Got cloudnets (should be the same as L3 agents):\n    %s", "\n    ".join(cloudnets))
