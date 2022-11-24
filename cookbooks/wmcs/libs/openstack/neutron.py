#!/usr/bin/env python3
"""Openstack Neutron specific related code."""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from cookbooks.wmcs.libs.common import CommandRunnerMixin
from cookbooks.wmcs.libs.openstack.common import (
    OpenstackAPI,
    OpenstackError,
    OpenstackID,
    OpenstackIdentifier,
    wait_for_it,
)

LOGGER = logging.getLogger(__name__)


class NeutronError(OpenstackError):
    """Neutron specific openstack error."""


class IncompleteData(NeutronError):
    """Thrown when trying to act on an object without having loaded all it's data."""


class CloudnetsUnhealthy(NeutronError):
    """Happens when some of the cloudnets are not in a healthy state."""


class CloudnetAdminDown(NeutronError):
    """Used to say the operation failed due to the cloudnet being admin down."""


class CloudnetAdminUp(NeutronError):
    """Used to say the operation failed due to the cloudnet being admin up."""


class NetworkUnhealthy(NeutronError):
    """Happens when there's not enough agents in one of the types to serve requests."""


class NeutronAgentType(Enum):
    """List of neutron agent types and their 'agent type' string.

    Extracted from 'neutron agent-list --format json' on a full installation. Note that they are case sensitive.
    """

    L3_AGENT = "L3 agent"
    LINUX_BRIDGE_AGENT = "Linux bridge agent"
    DHCP_AGENT = "DHCP agent"
    METADATA_AGENT = "Metadata agent"


class NeutronAlerts(Enum):
    """List of neutron alerts and their names."""

    NEUTRON_AGENT_DOWN = "NeutronAgentDown"


class NeutronAgentHAState(Enum):
    """HA state for a neutron agent."""

    ACTIVE = "active"
    STANDBY = "standby"


class NeutronRouterStatus(Enum):
    """Status of a neutron router.

    Gotten from https://github.com/openstack/neutron-lib/blob/master/neutron_lib/constants.py#L427
    """

    ACTIVE = "ACTIVE"
    ALLOCATING = "ALLOCATING"
    ERROR = "ERROR"


@dataclass(frozen=True)
class NeutronPartialRouter:
    """Partial info for a router as returned by router_list.

    We are only storing the fields we are using, if you need more please add them.
    """

    name: str
    router_id: str
    tenant_id: str
    has_ha: bool

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "NeutronPartialRouter":
        """Creates a NeutronPartialRouter from the json output of neutron router-list.

        Note that we only get the fields we use/find useful, add new whenever needed.

        Example of list_data:
        {
            "id": "5712e22e-134a-40d3-a75a-1c9b441717ad",
            "name": "cloudinstances2b-gw",
            "tenant_id": "admin",
            "external_gateway_info": {
            "network_id": "57017d7c-3817-429a-8aa3-b028de82cdcc",
            "external_fixed_ips": [
                {
                "subnet_id": "2596edb4-5a40-41b9-9e67-f1f9e40e329c",
                "ip_address": "185.15.57.10"
                }
            ],
            "enable_snat": false
            },
            "distributed": false,
            "ha": true
        }
        """
        return cls(
            name=data["name"],
            router_id=data["id"],
            tenant_id=data["tenant_id"],
            has_ha=data["ha"],
        )

    def __str__(self) -> str:
        """Return the string representation of this class."""
        return f"{self.name}: router_id:{self.router_id} tenant_id:{self.tenant_id} has_ha:{self.has_ha}"


@dataclass(frozen=True)
class NeutronRouter(NeutronPartialRouter):
    """Full Neutron router representation.

    Only storing the field we are using, if you need more please add them.
    """

    admin_state_up: bool
    status: NeutronRouterStatus

    def is_healthy(self) -> bool:
        """Given a router, check if it's up."""
        if self.admin_state_up is None or self.status is None:
            raise IncompleteData("Can't run is_healthy on a router returned by router_list, use router_show instead.")

        return bool(self.status == NeutronRouterStatus.ACTIVE and self.has_ha and self.admin_state_up)

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "NeutronRouter":
        """Create

        Example of show_data:
        {
            "admin_state_up": true,
            "availability_zone_hints": [],
            "availability_zones": [
                "nova"
            ],
            "created_at": "2018-03-29T14:18:50Z",
            "description": "",
            "distributed": false,
            "external_gateway_info": {
                "network_id": "57017d7c-3817-429a-8aa3-b028de82cdcc",
                "external_fixed_ips": [
                {
                    "subnet_id": "2596edb4-5a40-41b9-9e67-f1f9e40e329c",
                    "ip_address": "185.15.57.10"
                }
                ],
                "enable_snat": false
            },
            "flavor_id": null,
            "ha": true,
            "id": "5712e22e-134a-40d3-a75a-1c9b441717ad",
            "name": "cloudinstances2b-gw",
            "project_id": "admin",
            "revision_number": 24,
            "routes": [],
            "status": "ACTIVE",
            "tags": [],
            "tenant_id": "admin",
            "updated_at": "2022-04-27T16:35:05Z"
            }
        """
        return cls(
            name=data["name"],
            router_id=data["id"],
            tenant_id=data["tenant_id"],
            has_ha=data["ha"],
            admin_state_up=data["admin_state_up"],
            status=NeutronRouterStatus(data["status"]),
        )


@dataclass(frozen=True)
class NeutronAgent:
    """Neutron agent info."""

    agent_id: OpenstackID
    agent_type: NeutronAgentType
    host: str
    alive: bool
    admin_state_up: bool
    availability_zone: Optional[str] = None
    binary: Optional[str] = None
    ha_state: Optional[NeutronAgentHAState] = None

    @classmethod
    def from_agent_data(cls, agent_data: Dict[str, Any]) -> "NeutronAgent":
        """Get a NetworkAgent passing the agent_data as returned by the neutron cli."""
        return cls(
            host=agent_data["host"],
            agent_type=NeutronAgentType(agent_data["agent_type"]),
            admin_state_up=agent_data["admin_state_up"],
            alive=agent_data["alive"] == ":-)",
            agent_id=agent_data["id"],
            binary=agent_data.get("binary", None),
            availability_zone=agent_data.get("availability_zone", None),
            ha_state=NeutronAgentHAState(agent_data["ha_state"]) if "ha_state" in agent_data else None,
        )

    def __str__(self) -> str:
        """Return the string representation of this class."""
        return (
            f"{self.host} ({self.agent_type}): "
            f"{'ADMIN_UP' if self.admin_state_up else 'ADMIN_DOWN'} "
            f"{'ALIVE' if self.alive else 'DEAD'} "
            f"ha_state:{self.ha_state if self.ha_state is not None else 'NotFetched'} "
            f"id:{self.agent_id} "
            f"binary:{self.binary if self.binary is not None else 'NotFetched'}"
        )

    def is_healthy(self) -> bool:
        """Check if the agent is healthy."""
        return self.alive and self.admin_state_up


class NeutronController(CommandRunnerMixin):
    """Neutron specific controller"""

    def __init__(self, openstack_api: OpenstackAPI):
        """Controller to handle neutron commands and operations."""
        self.openstack_api = openstack_api
        self.control_node = openstack_api.control_node
        super().__init__(command_runner_node=self.control_node)

    def _get_full_command(self, *command: str, json_output: bool = True, project_as_arg: bool = False):
        cmd = ["source", "/root/novaenv.sh", "&&", "neutron", *command]
        if json_output:
            cmd.extend(["--format", "json"])

        script = " ".join(cmd)
        # we need sudo, and the sourced credentials, so we have to wrap it in a bash command
        return ["bash", "-c", f"'{script}'"]

    def run_formatted_as_list(self, *command: str, **kwargs: Any) -> List[Any]:
        """Run a neutron command on a control node forcing json output."""
        # neutron command return a first line in the output that is a warning, not part of the json
        kwargs["skip_first_line"] = True
        kwargs["print_output"] = False
        kwargs["print_progress_bars"] = False
        return super().run_formatted_as_list(*command, **kwargs)

    def run_formatted_as_dict(self, *command: str, **kwargs: Any) -> Dict[str, Any]:
        """Run a neutron command on a control node forcing json output."""
        kwargs["skip_first_line"] = True
        kwargs["print_output"] = False
        kwargs["print_progress_bars"] = False
        return super().run_formatted_as_dict(*command, **kwargs)

    def _run_one_raw(self, *command: str, **kwargs: Any) -> str:
        """Run a neutron command on a control node returning the raw string."""
        kwargs["print_output"] = False
        kwargs["print_progress_bars"] = False
        return super().run_raw(*command, **kwargs)

    def agent_list(self) -> List[NeutronAgent]:
        """Get the list of neutron agents."""
        return [
            NeutronAgent.from_agent_data(agent_data=agent_data)
            for agent_data in self.run_formatted_as_list("agent-list", is_safe=True)
        ]

    def agent_set_admin_up(self, agent_id: OpenstackID) -> None:
        """Set the given agent as admin-state-up (online)."""
        self._run_one_raw("agent-update", "--admin-state-up", agent_id, json_output=False)

    def agent_set_admin_down(self, agent_id: OpenstackID) -> None:
        """Set the given agent as admin-state-down (offline)."""
        self._run_one_raw("agent-update", "--admin-state-down", agent_id, json_output=False)

    def cloudnet_set_admin_down(self, cloudnet_host: str) -> None:
        """Given a cloudnet hostname, set all it's agents down, usually for maintenance or reboot."""
        cloudnet_agents = [agent for agent in self.agent_list() if agent.host == cloudnet_host]
        for agent in cloudnet_agents:
            if agent.admin_state_up:
                self.agent_set_admin_down(agent_id=agent.agent_id)

        self.wait_for_cloudnet_admin_down(cloudnet_host=cloudnet_host)

    def cloudnet_set_admin_up(self, cloudnet_host: str) -> None:
        """Given a cloudnet hostname, set all it's agents up, usually after maintenance or reboot."""
        cloudnet_agents = [agent for agent in self.agent_list() if agent.host == cloudnet_host]
        for agent in cloudnet_agents:
            if not agent.admin_state_up:
                self.agent_set_admin_up(agent_id=agent.agent_id)

        self.wait_for_cloudnet_admin_up(cloudnet_host=cloudnet_host)

    def wait_for_cloudnet_admin_down(self, cloudnet_host: str) -> None:
        """Wait until the given cloudnet is set as admin down."""

        def cloudnet_admin_down():
            all_agents = self.agent_list()
            cloudnet_agents = [agent for agent in all_agents if agent.host == cloudnet_host]
            return all(not agent.admin_state_up for agent in cloudnet_agents)

        wait_for_it(
            condition_fn=cloudnet_admin_down,
            condition_name_msg="Cloudnet set as admin down",
            when_failed_raise_exception=CloudnetAdminUp,
            condition_failed_msg_fn=lambda: "Some cloudnet agents did not turn admin down.",
        )

    def wait_for_cloudnet_admin_up(self, cloudnet_host: str) -> None:
        """Wait until the given cloudnet is set as admin up."""

        def cloudnet_admin_up():
            all_agents = self.agent_list()
            cloudnet_agents = [agent for agent in all_agents if agent.host == cloudnet_host]
            return all(agent.admin_state_up for agent in cloudnet_agents)

        wait_for_it(
            condition_fn=cloudnet_admin_up,
            condition_name_msg="Cloudnet set as admin up",
            when_failed_raise_exception=CloudnetAdminDown,
            condition_failed_msg_fn=lambda: "Some cloudnet agents did not turn admin up.",
        )

    def router_list(self) -> List[NeutronPartialRouter]:
        """Get the list of neutron routers."""
        return [
            NeutronPartialRouter.from_data(data=list_data)
            for list_data in self.run_formatted_as_list("router-list", is_safe=True)
        ]

    def router_show(self, router: OpenstackIdentifier) -> NeutronRouter:
        """Show details of the given router."""
        return NeutronRouter.from_data(data=self.run_formatted_as_dict("router-show", router, is_safe=True))

    def list_agents_hosting_router(self, router: OpenstackIdentifier) -> List[NeutronAgent]:
        """Get the list of nodes hosting a given router routers."""
        return [
            NeutronAgent.from_agent_data(agent_data={**agent_data, "agent_type": NeutronAgentType.L3_AGENT.value})
            for agent_data in self.run_formatted_as_list("l3-agent-list-hosting-router", router, is_safe=True)
        ]

    def get_cloudnets(self) -> List[str]:
        """Retrieves the known cloudnets.

        Currently does that by checking the neutron agents running on those.
        """
        return [agent.host for agent in self.agent_list() if agent.agent_type == NeutronAgentType.L3_AGENT]

    def list_routers_on_agent(self, agent_id: OpenstackID) -> List[Dict[str, Any]]:
        """Get the list of routers hosted a given agent."""
        return self.run_formatted_as_list("router-list-on-l3-agent", agent_id, is_safe=True)

    def check_if_network_is_alive(self) -> None:
        """Check if the network is in a working state (all agents up and running, all routers up and running).

        Raises:
            NetworkUnhealthy if the network is not OK.

        """
        cloudnets = self.get_cloudnets()
        cloudnet_agents = [agent for agent in self.agent_list() if agent.host in cloudnets]
        for agent in cloudnet_agents:
            if not agent.admin_state_up or not agent.alive:
                agents_str = "\n".join(str(agent) for agent in cloudnet_agents)
                raise NetworkUnhealthy(f"Some agents are not healthy:\n{agents_str}")

        all_routers = self.router_list()
        for partial_router in all_routers:
            full_router = self.router_show(router=partial_router.name)
            if not full_router.is_healthy():
                raise NetworkUnhealthy(f"Router {full_router.name} is not healthy:\n{full_router}")

    def wait_for_l3_handover(self):
        """Wait until there's one primary for all l3 agents.

        Used to make sure the network is working after taking one l3 agent down.
        """

        def all_routers_have_active_agent() -> bool:
            routers_down = []
            routers = self.router_list()
            for router in routers:
                agents_on_router = self.list_agents_hosting_router(router=router.router_id)
                if not any(
                    agent.admin_state_up and agent.alive and agent.ha_state == NeutronAgentHAState.ACTIVE
                    for agent in agents_on_router
                ):
                    routers_down.append(router)
            return len(routers_down) == 0

        wait_for_it(
            condition_fn=all_routers_have_active_agent,
            condition_name_msg="all routers have a primary agent running",
            when_failed_raise_exception=NetworkUnhealthy,
            condition_failed_msg_fn=lambda: "Some routers have no primary agents",
        )

    def get_l3_primary(self) -> str:
        """Returns the cloudnet host that is primary for all l3 routers.

        NOTE: We expect all the routers to have the same primary (we only have one router for now), once we have more
        or the primaries are mixed, this should be changed.
        """
        routers = self.router_list()
        for router in routers:
            agents_on_router = self.list_agents_hosting_router(router=router.router_id)
            for agent in agents_on_router:
                if agent.admin_state_up and agent.alive and agent.ha_state == NeutronAgentHAState.ACTIVE:
                    return agent.host

            raise NeutronError(f"Unable to find primary agent for router {router}, known agents: {agents_on_router}")

        raise NeutronError("No routers found.")

    def wait_for_network_alive(self, timeout_seconds: int = 900):
        """Wait until the network is up and running again."""

        def is_network_alive():
            try:
                self.check_if_network_is_alive()
            except NetworkUnhealthy:
                return False

            return True

        wait_for_it(
            condition_fn=is_network_alive,
            when_failed_raise_exception=NetworkUnhealthy,
            condition_name_msg="network is alive",
            condition_failed_msg_fn=lambda: "Some agents are not running",
            timeout_seconds=timeout_seconds,
        )

    def is_router_healthy(self, router_id: OpenstackIdentifier) -> bool:
        """Given a router, check if it's up."""
        router = self.router_show(router=router_id)
        return bool(router.status == NeutronRouterStatus.ACTIVE and router.has_ha and router.admin_state_up)
