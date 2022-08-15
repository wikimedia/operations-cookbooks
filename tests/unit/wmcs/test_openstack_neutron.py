from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import cumin
import pytest

from cookbooks.wmcs.libs.common import TestUtils
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI
from cookbooks.wmcs.libs.openstack.neutron import (
    NetworkUnhealthy,
    NeutronAgent,
    NeutronAgentHAState,
    NeutronAgentType,
    NeutronController,
    NeutronPartialRouter,
    NeutronRouter,
    NeutronRouterStatus,
)


def get_stub_agent(
    agent_id: str = "dummyagentid",
    agent_type: NeutronAgentType = NeutronAgentType.L3_AGENT,
    ha_state: Optional[NeutronAgentHAState] = None,
    host: str = "dummyhost",
    availability_zone: Optional[str] = "dummyavailabilityzone",
    binary: Optional[str] = "dummybinary",
    admin_state_up: bool = True,
    alive: bool = True,
) -> NeutronAgent:
    return NeutronAgent(
        agent_id=agent_id,
        agent_type=agent_type,
        ha_state=ha_state,
        host=host,
        availability_zone=availability_zone,
        binary=binary,
        admin_state_up=admin_state_up,
        alive=alive,
    )


def partial_router_from_full_router(router: NeutronRouter) -> NeutronPartialRouter:
    return NeutronPartialRouter(
        has_ha=router.has_ha,
        router_id=router.router_id,
        name=router.name,
        tenant_id=router.tenant_id,
    )


def get_stub_router(
    router_id: str = "dummyrouterid",
    status: NeutronRouterStatus = NeutronRouterStatus.ACTIVE,
    has_ha: bool = True,
    admin_state_up: bool = True,
) -> NeutronRouter:
    return NeutronRouter(
        admin_state_up=admin_state_up,
        has_ha=has_ha,
        router_id=router_id,
        name="cloudinstances2b-gw",
        status=status,
        tenant_id="admin",
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No agents": {
                # neutron expects a first spurious line
                "neutron_output": "\n[]",
                "expected_agents": [],
            },
            "L3 agent": {
                "neutron_output": """
                    [
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "agent_type": "L3 agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-l3-agent"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="4be214c8-76ef-40f8-9d5d-4c344d213311",
                        agent_type=NeutronAgentType.L3_AGENT,
                        host="cloudnet1003",
                        availability_zone="nova",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-l3-agent",
                    )
                ],
            },
            "Linux bridge agent": {
                "neutron_output": """
                    [
                          {
                            "id": "ce4f0afc-d0c0-411b-9faa-9e4f83c746b0",
                            "agent_type": "Linux bridge agent",
                            "host": "cloudvirt1036",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-linuxbridge-agent"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="ce4f0afc-d0c0-411b-9faa-9e4f83c746b0",
                        agent_type=NeutronAgentType.LINUX_BRIDGE_AGENT,
                        host="cloudvirt1036",
                        availability_zone="",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-linuxbridge-agent",
                    )
                ],
            },
            "Metadata agent": {
                "neutron_output": """
                    [
                        {
                            "id": "d475e07d-52b3-476e-9a4f-e63b21e1075e",
                            "agent_type": "Metadata agent",
                            "host": "cloudnet1004",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-metadata-agent"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="d475e07d-52b3-476e-9a4f-e63b21e1075e",
                        agent_type=NeutronAgentType.METADATA_AGENT,
                        host="cloudnet1004",
                        availability_zone="",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-metadata-agent",
                    )
                ],
            },
            "DHCP agent": {
                "neutron_output": """
                    [
                        {
                            "id": "0b2f519f-a5ab-4188-82bf-01431810d55a",
                            "agent_type": "DHCP agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-dhcp-agent"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="0b2f519f-a5ab-4188-82bf-01431810d55a",
                        agent_type=NeutronAgentType.DHCP_AGENT,
                        host="cloudnet1003",
                        availability_zone="nova",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-dhcp-agent",
                    )
                ],
            },
            "More than one agent": {
                "neutron_output": """
                    [
                        {
                            "id": "0b2f519f-a5ab-4188-82bf-01431810d55a",
                            "agent_type": "DHCP agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-dhcp-agent"
                        },
                        {
                            "id": "d475e07d-52b3-476e-9a4f-e63b21e1075e",
                            "agent_type": "Metadata agent",
                            "host": "cloudnet1004",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-metadata-agent"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="0b2f519f-a5ab-4188-82bf-01431810d55a",
                        agent_type=NeutronAgentType.DHCP_AGENT,
                        host="cloudnet1003",
                        availability_zone="nova",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-dhcp-agent",
                    ),
                    NeutronAgent(
                        agent_id="d475e07d-52b3-476e-9a4f-e63b21e1075e",
                        agent_type=NeutronAgentType.METADATA_AGENT,
                        host="cloudnet1004",
                        availability_zone="",
                        alive=True,
                        admin_state_up=True,
                        binary="neutron-metadata-agent",
                    ),
                ],
            },
        }
    )
)
def test_NeutronController_agent_list_works(neutron_output: str, expected_agents: List[NeutronAgent]):
    fake_remote = TestUtils.get_fake_remote(responses=[neutron_output])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    fake_run_sync = fake_remote.query.return_value.run_sync

    gotten_agents = my_controller.agent_list()

    assert gotten_agents == expected_agents
    fake_run_sync.assert_called_with(
        cumin.transports.Command("bash -c 'source /root/novaenv.sh && neutron agent-list --format json'", ok_codes=[0]),
        is_safe=False,
        print_output=False,
        print_progress_bars=False,
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No routers": {
                # neutron expects a first line that will be discarded
                "neutron_output": "\n[]",
                "expected_routers": [],
            },
            "One router": {
                "neutron_output": """
                    [
                        {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                            "name": "cloudinstances2b-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                                "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                                "external_fixed_ips": [
                                    {
                                    "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51",
                                    "ip_address": "185.15.56.238"
                                    }
                                ],
                                "enable_snat": false
                            },
                            "distributed": false,
                            "ha": true
                        }
                    ]
                """,
                "expected_routers": [
                    NeutronPartialRouter(
                        router_id="d93771ba-2711-4f88-804a-8df6fd03978a",
                        name="cloudinstances2b-gw",
                        tenant_id="admin",
                        has_ha=True,
                    )
                ],
            },
            "Many routers": {
                "neutron_output": """
                    [
                        {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                            "name": "cloudinstances2b-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                                "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                                "external_fixed_ips": [
                                    {
                                    "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51",
                                    "ip_address": "185.15.56.238"
                                    }
                                ],
                                "enable_snat": false
                            },
                            "distributed": false,
                            "ha": true
                        },
                        {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978b",
                            "name": "cloudinstances2c-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                                "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75124",
                                "external_fixed_ips": [
                                    {
                                    "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f52",
                                    "ip_address": "185.15.56.239"
                                    }
                                ],
                                "enable_snat": false
                            },
                            "distributed": false,
                            "ha": true
                        }
                    ]
                """,
                "expected_routers": [
                    NeutronPartialRouter(
                        router_id="d93771ba-2711-4f88-804a-8df6fd03978a",
                        name="cloudinstances2b-gw",
                        tenant_id="admin",
                        has_ha=True,
                    ),
                    NeutronPartialRouter(
                        router_id="d93771ba-2711-4f88-804a-8df6fd03978b",
                        name="cloudinstances2c-gw",
                        tenant_id="admin",
                        has_ha=True,
                    ),
                ],
            },
        }
    )
)
def test_NeutronController_router_list_works(neutron_output: str, expected_routers: List[NeutronAgent]):
    fake_remote = TestUtils.get_fake_remote(responses=[neutron_output])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    fake_run_sync = fake_remote.query.return_value.run_sync

    gotten_routers = my_controller.router_list()

    assert gotten_routers == expected_routers
    fake_run_sync.assert_called_with(
        cumin.transports.Command(
            "bash -c 'source /root/novaenv.sh && neutron router-list --format json'",
            ok_codes=[0],
        ),
        is_safe=False,
        print_output=False,
        print_progress_bars=False,
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No nodes": {
                # neutron expects a first spurious line
                "neutron_output": "\n[]",
                "expected_agents": [],
            },
            "One node": {
                "neutron_output": """
                    [
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "host": "cloudnet1003",
                            "admin_state_up": true,
                            "alive": ":-)",
                            "ha_state": "standby"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_type=NeutronAgentType.L3_AGENT,
                        agent_id="4be214c8-76ef-40f8-9d5d-4c344d213311",
                        host="cloudnet1003",
                        admin_state_up=True,
                        alive=True,
                        ha_state=NeutronAgentHAState.STANDBY,
                    ),
                ],
            },
            "Many nodes": {
                "neutron_output": """
                    [
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "host": "cloudnet1003",
                            "admin_state_up": true,
                            "alive": ":-)",
                            "ha_state": "standby"
                        },
                        {
                            "id": "970df1d1-505d-47a4-8d35-1b13c0dfe098",
                            "host": "cloudnet1004",
                            "admin_state_up": true,
                            "alive": ":-)",
                            "ha_state": "active"
                        }
                    ]
                """,
                "expected_agents": [
                    NeutronAgent(
                        agent_id="4be214c8-76ef-40f8-9d5d-4c344d213311",
                        host="cloudnet1003",
                        admin_state_up=True,
                        alive=True,
                        ha_state=NeutronAgentHAState.STANDBY,
                        agent_type=NeutronAgentType.L3_AGENT,
                    ),
                    NeutronAgent(
                        agent_id="970df1d1-505d-47a4-8d35-1b13c0dfe098",
                        host="cloudnet1004",
                        admin_state_up=True,
                        alive=True,
                        ha_state=NeutronAgentHAState.ACTIVE,
                        agent_type=NeutronAgentType.L3_AGENT,
                    ),
                ],
            },
        }
    )
)
def test_NeutronController_list_agents_hosting_router_works(neutron_output: str, expected_agents: List[Dict[str, Any]]):
    fake_remote = TestUtils.get_fake_remote(responses=[neutron_output])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    fake_run_sync = fake_remote.query.return_value.run_sync

    gotten_agents = my_controller.list_agents_hosting_router(router="dummy_router")

    assert gotten_agents == expected_agents
    fake_run_sync.assert_called_with(
        cumin.transports.Command(
            "bash -c 'source /root/novaenv.sh && neutron l3-agent-list-hosting-router dummy_router --format json'",
            ok_codes=[0],
        ),
        is_safe=False,
        print_output=False,
        print_progress_bars=False,
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No nodes": {
                # neutron expects a first spurious line
                "neutron_output": "\n[]",
                "expected_routers": [],
            },
            "One router": {
                "neutron_output": """
                    [
                          {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                            "name": "cloudinstances2b-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                            "external_fixed_ips": [
                                {
                                "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51",
                                "ip_address": "185.15.56.238"
                                }
                            ],
                            "enable_snat": false
                            }
                        }
                    ]
                """,
                "expected_routers": [
                    {
                        "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                        "name": "cloudinstances2b-gw",
                        "tenant_id": "admin",
                        "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                            "external_fixed_ips": [
                                {"subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51", "ip_address": "185.15.56.238"}
                            ],
                            "enable_snat": False,
                        },
                    }
                ],
            },
            "Many routers": {
                "neutron_output": """
                    [
                          {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                            "name": "cloudinstances2b-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                            "external_fixed_ips": [
                                {
                                "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51",
                                "ip_address": "185.15.56.238"
                                }
                            ],
                            "enable_snat": false
                            }
                        },
                          {
                            "id": "d93771ba-2711-4f88-804a-8df6fd03978b",
                            "name": "cloudinstances2c-gw",
                            "tenant_id": "admin",
                            "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75124",
                            "external_fixed_ips": [
                                {
                                "subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f52",
                                "ip_address": "185.15.56.239"
                                }
                            ],
                            "enable_snat": false
                            }
                        }
                    ]
                """,
                "expected_routers": [
                    {
                        "id": "d93771ba-2711-4f88-804a-8df6fd03978a",
                        "name": "cloudinstances2b-gw",
                        "tenant_id": "admin",
                        "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75123",
                            "external_fixed_ips": [
                                {"subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f51", "ip_address": "185.15.56.238"}
                            ],
                            "enable_snat": False,
                        },
                    },
                    {
                        "id": "d93771ba-2711-4f88-804a-8df6fd03978b",
                        "name": "cloudinstances2c-gw",
                        "tenant_id": "admin",
                        "external_gateway_info": {
                            "network_id": "5c9ee953-3a19-4e84-be0f-069b5da75124",
                            "external_fixed_ips": [
                                {"subnet_id": "77dba34f-c8f2-4706-a0b6-2a8ed4d91f52", "ip_address": "185.15.56.239"}
                            ],
                            "enable_snat": False,
                        },
                    },
                ],
            },
        }
    )
)
def test_NeutronController_list_routers_on_agent_works(neutron_output: str, expected_routers: List[Dict[str, Any]]):
    fake_remote = TestUtils.get_fake_remote(responses=[neutron_output])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    fake_run_sync = fake_remote.query.return_value.run_sync

    gotten_nodes = my_controller.list_routers_on_agent(agent_id="some-agent-id")

    assert gotten_nodes == expected_routers
    fake_run_sync.assert_called_with(
        cumin.transports.Command(
            "bash -c 'source /root/novaenv.sh && neutron router-list-on-l3-agent some-agent-id --format json'",
            ok_codes=[0],
        ),
        is_safe=False,
        print_output=False,
        print_progress_bars=False,
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No cloudnets": {
                # neutron expects a first spurious line
                "neutron_output": "\n[]",
                "expected_cloudnets": [],
            },
            "Linux bridge agent": {
                "neutron_output": """
                    [
                          {
                            "id": "ce4f0afc-d0c0-411b-9faa-9e4f83c746b0",
                            "agent_type": "Linux bridge agent",
                            "host": "cloudvirt1036",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-linuxbridge-agent"
                        }
                    ]
                """,
                "expected_cloudnets": [],
            },
            "Metadata agent": {
                "neutron_output": """
                    [
                        {
                            "id": "d475e07d-52b3-476e-9a4f-e63b21e1075e",
                            "agent_type": "Metadata agent",
                            "host": "cloudnet1004",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-metadata-agent"
                        }
                    ]
                """,
                "expected_cloudnets": [],
            },
            "DHCP agent": {
                "neutron_output": """
                    [
                        {
                            "id": "0b2f519f-a5ab-4188-82bf-01431810d55a",
                            "agent_type": "DHCP agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-dhcp-agent"
                        }
                    ]
                """,
                "expected_cloudnets": [],
            },
            "L3 agent": {
                "neutron_output": """
                    [
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "agent_type": "L3 agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-l3-agent"
                        }
                    ]
                """,
                "expected_cloudnets": ["cloudnet1003"],
            },
            "More than one agent": {
                "neutron_output": """
                    [
                        {
                            "id": "0b2f519f-a5ab-4188-82bf-01431810d55a",
                            "agent_type": "DHCP agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-dhcp-agent"
                        },
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "agent_type": "L3 agent",
                            "host": "cloudnet1004",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-l3-agent"
                        },
                        {
                            "id": "4be214c8-76ef-40f8-9d5d-4c344d213311",
                            "agent_type": "L3 agent",
                            "host": "cloudnet1003",
                            "availability_zone": "nova",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-l3-agent"
                        },
                        {
                            "id": "d475e07d-52b3-476e-9a4f-e63b21e1075e",
                            "agent_type": "Metadata agent",
                            "host": "cloudnet1004",
                            "availability_zone": "",
                            "alive": ":-)",
                            "admin_state_up": true,
                            "binary": "neutron-metadata-agent"
                        }
                    ]
                """,
                "expected_cloudnets": ["cloudnet1003", "cloudnet1004"],
            },
        }
    )
)
def test_NeutronController_get_cloudnets_works(neutron_output: str, expected_cloudnets: List[str]):
    fake_remote = TestUtils.get_fake_remote(responses=[neutron_output])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    fake_run_sync = fake_remote.query.return_value.run_sync

    gotten_agents = my_controller.get_cloudnets()

    assert sorted(gotten_agents) == sorted(expected_cloudnets)
    fake_run_sync.assert_called_with(
        cumin.transports.Command(
            "bash -c 'source /root/novaenv.sh && neutron agent-list --format json'",
            ok_codes=[0],
        ),
        is_safe=False,
        print_output=False,
        print_progress_bars=False,
    )


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "No agents and no routers": {
                "agents": [],
                "routers": [],
            },
            "All agent and routers ok": {
                "agents": [
                    get_stub_agent(agent_id="agent1", admin_state_up=True, alive=True),
                    get_stub_agent(agent_id="agent2", admin_state_up=True, alive=True),
                ],
                "routers": [
                    get_stub_router(router_id="router1", admin_state_up=True, has_ha=True),
                    get_stub_router(router_id="router2", admin_state_up=True, has_ha=True),
                ],
            },
        }
    )
)
def test_NeutronController_check_if_network_is_alive_does_not_raise(
    agents: List[NeutronAgent], routers: List[NeutronRouter]
):
    # just in case a call gets through
    fake_remote = TestUtils.get_fake_remote(responses=[])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    partial_routers = [partial_router_from_full_router(router) for router in routers]

    with patch.object(my_controller, "agent_list", MagicMock(return_value=agents)), patch.object(
        my_controller, "router_list", MagicMock(return_value=partial_routers)
    ), patch.object(my_controller, "router_show", MagicMock(side_effect=routers)):

        # assert it does not raise
        my_controller.check_if_network_is_alive()


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "One agent dead, routers ok": {
                "agents": [
                    get_stub_agent(agent_id="agent1", admin_state_up=True, alive=True),
                    get_stub_agent(agent_id="agent2", admin_state_up=True, alive=False),
                ],
                "routers": [
                    get_stub_router(router_id="router1", admin_state_up=True, has_ha=True),
                    get_stub_router(router_id="router2", admin_state_up=True, has_ha=True),
                ],
            },
            "One agent admin down, routers ok": {
                "agents": [
                    get_stub_agent(agent_id="agent1", admin_state_up=True, alive=True),
                    get_stub_agent(agent_id="agent2", admin_state_up=False, alive=True),
                ],
                "routers": [
                    get_stub_router(router_id="router1", admin_state_up=True, has_ha=True),
                    get_stub_router(router_id="router2", admin_state_up=True, has_ha=True),
                ],
            },
            "Agents ok, one router not ha": {
                "agents": [
                    get_stub_agent(agent_id="agent1", admin_state_up=True, alive=True),
                    get_stub_agent(agent_id="agent2", admin_state_up=True, alive=True),
                ],
                "routers": [
                    get_stub_router(router_id="router1", admin_state_up=True, has_ha=True),
                    get_stub_router(router_id="router2", admin_state_up=True, has_ha=False),
                ],
            },
            "Agents ok, one router admin down": {
                "agents": [
                    get_stub_agent(agent_id="agent1", admin_state_up=True, alive=True),
                    get_stub_agent(agent_id="agent2", admin_state_up=True, alive=True),
                ],
                "routers": [
                    get_stub_router(router_id="router1", admin_state_up=True, has_ha=True),
                    get_stub_router(router_id="router2", admin_state_up=False, has_ha=True),
                ],
            },
        }
    )
)
def test_NeutronController_check_if_network_is_alive_raises(agents: List[NeutronAgent], routers: List[NeutronRouter]):
    # just in case a call gets through
    fake_remote = TestUtils.get_fake_remote(responses=[])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.EQIAD1)
    my_controller = NeutronController(openstack_api=my_api)
    partial_routers = [partial_router_from_full_router(router) for router in routers]

    with patch.object(my_controller, "agent_list", MagicMock(return_value=agents)), patch.object(
        my_controller, "router_list", MagicMock(return_value=partial_routers)
    ), patch.object(my_controller, "router_show", MagicMock(side_effect=routers)):

        with pytest.raises(NetworkUnhealthy):
            my_controller.check_if_network_is_alive()
