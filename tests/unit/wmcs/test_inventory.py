from typing import Dict, Type
from unittest import mock

import pytest

from cookbooks.wmcs.libs.common import TestUtils
from cookbooks.wmcs.libs.inventory import (
    CephNodeRoleName,
    Cluster,
    ClusterName,
    ClusterType,
    NodeInventoryInfo,
    NodeRoleName,
    OpenstackCluster,
    OpenstackClusterName,
    OpenstackNodeRoleName,
    Site,
    SiteName,
    get_node_inventory_info,
)


def get_dummy_inventory(
    node_fqdn: str = "dummy.no.de",
    site_name: SiteName = SiteName.CODFW,
    cluster_type: ClusterType = ClusterType.OPENSTACK,
    cluster_name: ClusterName = OpenstackClusterName.CODFW1DEV,
    cluster_class: Type[Cluster] = OpenstackCluster,
    role_name: NodeRoleName = OpenstackNodeRoleName.CONTROL,
) -> Dict[SiteName, Site]:
    return {
        site_name: Site(
            name=site_name,
            clusters_by_type={
                cluster_type: {
                    cluster_name: cluster_class(
                        name=cluster_name,
                        nodes_by_role={role_name: [node_fqdn]},
                    )
                }
            },
        )
    }


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "Node in inventory matches inventory site": {
                "node_fqdn": "something.some.where",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
                "inventory": get_dummy_inventory(
                    node_fqdn="something.some.where",
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
            },
            "Node in inventory matches inventory site, even if wikimedia.org domain": {
                "node_fqdn": "something.wikimedia.org",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
                "inventory": get_dummy_inventory(
                    node_fqdn="something.wikimedia.org",
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
            },
            "Node in inventory matches inventory site, even if wrong numeration": {
                "node_fqdn": "something1001.wikimedia.org",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
                "inventory": get_dummy_inventory(
                    node_fqdn="something1001.wikimedia.org",
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.OPENSTACK,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=OpenstackNodeRoleName.CONTROL,
                ),
            },
            "Node not in inventory, with eqiad.wmnet matches eqiad site": {
                "node_fqdn": "something.eqiad.wmnet",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.EQIAD,
                ),
                "inventory": get_dummy_inventory(),
            },
            "Node not in inventory, with unknown domain, matches by number 2001 -> codfw": {
                "node_fqdn": "something2001.some.where",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.CODFW,
                ),
                "inventory": get_dummy_inventory(),
            },
            "Node not in inventory, with unknown domain, matches by number 1001 -> eqaid": {
                "node_fqdn": "something1001.some.where",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.EQIAD,
                ),
                "inventory": get_dummy_inventory(),
            },
            (
                "Node not in inventory, with correct domain, and cloudcephosd name matches correct role_name and "
                "cluster_type"
            ): {
                "node_fqdn": "cloudcephosd2001.codfw.wmnet",
                "expected_node_inventory_info": NodeInventoryInfo(
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.CEPH,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=CephNodeRoleName.OSD,
                ),
                "inventory": get_dummy_inventory(
                    node_fqdn="some_other.host",
                    site_name=SiteName.CODFW,
                    cluster_type=ClusterType.CEPH,
                    cluster_name=OpenstackClusterName.CODFW1DEV,
                    role_name=CephNodeRoleName.OSD,
                ),
            },
        }
    )
)
def test_get_node_inventory_info(
    node_fqdn: str, expected_node_inventory_info: NodeInventoryInfo, inventory: Dict[SiteName, Site]
):
    with mock.patch("cookbooks.wmcs.libs.inventory.get_inventory", return_value=inventory):
        gotten_inventory_info = get_node_inventory_info(node=node_fqdn)

    assert gotten_inventory_info == expected_node_inventory_info
