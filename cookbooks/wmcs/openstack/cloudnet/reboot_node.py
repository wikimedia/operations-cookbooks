r"""WMCS Openstack - Reboot a cloudnet node .

Usage example:
    cookbook wmcs.openstack.cloudnet.reboot_node \
    --fqdn-to-reboot cloudnet1004.eqiad.wmnet

"""
import argparse
import logging
from datetime import datetime

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.alerts import downtime_alert, downtime_host, uptime_alert, uptime_host
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI, get_node_cluster_name
from cookbooks.wmcs.libs.openstack.neutron import NetworkUnhealthy, NeutronAgentType, NeutronAlerts, NeutronController

LOGGER = logging.getLogger(__name__)


class RebootNode(CookbookBase):
    """WMCS Openstack cookbook to reboot a single cloudnets, handling failover."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
        parser.add_argument(
            "--fqdn-to-reboot",
            required=True,
            help="FQDN of the node to reboot.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RebootNodeRunner,)(
            fqdn_to_reboot=args.fqdn_to_reboot,
            force=args.force,
            spicerack=self.spicerack,
        )


class RebootNodeRunner(CookbookRunnerBase):
    """Runner for RebootNode"""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn_to_reboot: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.fqdn_to_reboot = fqdn_to_reboot
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        cluster_name = get_node_cluster_name(node=self.fqdn_to_reboot)
        self.openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            cluster_name=cluster_name,
            project=self.common_opts.project,
        )
        self.neutron_controller = NeutronController(openstack_api=self.openstack_api)

        LOGGER.info("Checking that the current network setup is something we can handle...")
        self._check_network_setup_as_expected()

        try:
            self.neutron_controller.check_if_network_is_alive()
        except NetworkUnhealthy as error:
            if not self.force:
                raise Exception(
                    "There's some agent down in the network, if you still want to reboot the nodes pass --force."
                ) from error

            LOGGER.warning("Some agents are down, will continue due to --force: \n%s", error)

    def _check_network_setup_as_expected(self) -> None:
        l3_agents = [
            agent for agent in self.neutron_controller.agent_list() if agent.agent_type == NeutronAgentType.L3_AGENT
        ]
        # currently this is the same, but adding the check in case anything changes
        cloudnets = self.neutron_controller.get_cloudnets()
        if not cloudnets:
            raise Exception("No cloudnets found :-S")

        if len(cloudnets) != len(l3_agents):
            agent_hosts = [agent.host for agent in l3_agents]
            raise Exception(f"Got different cloudnets ({cloudnets}) than l3 agents ({agent_hosts})")

        for agent in l3_agents:
            if agent.host not in cloudnets:
                raise Exception(f"Agent {agent.host} not in cloudnets ({cloudnets})")

            routers = self.neutron_controller.list_routers_on_agent(agent.agent_id)
            if len(routers) != 1:
                raise Exception(f"Got more than one router on agent {agent.host}: {routers}")

    def run(self) -> None:
        """Main entry point"""
        self.sallogger.log(f"Rebooting cloudnet host {self.fqdn_to_reboot}")
        silence_id = downtime_alert(
            spicerack=self.spicerack,
            alert_name=NeutronAlerts.NEUTRON_AGENT_DOWN.value,
            task_id=self.common_opts.task_id,
            comment=f"Rebooting cloudnet {self.fqdn_to_reboot}",
        )

        node = self.spicerack.remote().query(f"D{{{self.fqdn_to_reboot}}}", use_sudo=True)
        host_name = self.fqdn_to_reboot.split(".", 1)[0]
        host_silence_id = downtime_host(
            spicerack=self.spicerack,
            host_name=host_name,
            comment="Rebooting with wmcs.openstack.cloudnet.reboot_node",
            task_id=self.common_opts.task_id,
        )

        LOGGER.info("Taking the node out of the cluster (setting admin-state-down to all it's agents)")
        self.neutron_controller.cloudnet_set_admin_down(cloudnet_host=host_name)
        if not self.force:
            agents_on_cloudnet = [agent for agent in self.neutron_controller.agent_list() if agent.host == host_name]
            if any(agent.agent_type == NeutronAgentType.L3_AGENT for agent in agents_on_cloudnet):
                LOGGER.info("This is an L3 agent node, waiting for the router handover if needed...")
                self.neutron_controller.wait_for_l3_handover()
                LOGGER.info("Handover done.")
        else:
            LOGGER.warning("Skipping L3 handover due to --force passed.")

        reboot_time = datetime.utcnow()
        node.reboot()

        node.wait_reboot_since(since=reboot_time)
        LOGGER.info(
            "Rebooted node %s, waiting for cluster to stabilize...",
            self.fqdn_to_reboot,
        )

        LOGGER.info("Making the host %s admin up...", host_name)
        self.neutron_controller.cloudnet_set_admin_up(cloudnet_host=host_name)
        LOGGER.info("Host %s is admin up", host_name)

        if not self.force:
            LOGGER.info("Waiting, for all it's agents to be up and running...")
            self.neutron_controller.wait_for_network_alive()
            LOGGER.info("All agents up.")
            LOGGER.info("Node up and running, and all agents working! Removing alert silences...")
        else:
            LOGGER.warning("Skipping waiting for the network alive due to --force passed")

        uptime_host(spicerack=self.spicerack, host_name=host_name, silence_id=host_silence_id)
        uptime_alert(spicerack=self.spicerack, silence_id=silence_id)
        LOGGER.info("Silences removed.")

        self.sallogger.log(f"Rebooted cloudnet host {self.fqdn_to_reboot}")
