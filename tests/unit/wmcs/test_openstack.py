from unittest import mock

import cumin
import pytest

from cookbooks.wmcs.libs.common import TestUtils
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import (
    OpenstackAPI,
    OpenstackBadQuota,
    OpenstackQuotaEntry,
    OpenstackQuotaName,
    Unit,
)


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "G to M": {
                "from_unit": Unit.GIGA,
                "expected_unit": Unit.MEGA,
            },
            "M to K": {
                "from_unit": Unit.MEGA,
                "expected_unit": Unit.KILO,
            },
            "K to B": {
                "from_unit": Unit.KILO,
                "expected_unit": Unit.UNIT,
            },
        }
    )
)
def test_Unit_next_unit_works(from_unit: str, expected_unit: str):
    gotten_unit = from_unit.next_unit()
    assert gotten_unit == expected_unit


def test_Unit_next_unit_raises_when_last_unit():
    with pytest.raises(OpenstackBadQuota):
        Unit.UNIT.next_unit()


# Test only a couple very used ones
@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "Gigabytes": {
                "quota_name": OpenstackQuotaName.GIGABYTES,
                "value": "3G",
                "expected_cli": "--gigabytes=3G",
            },
            "Per-volume gigabytes": {
                "quota_name": OpenstackQuotaName.PER_VOLUME_GIGABYTES,
                "value": "4G",
                "expected_cli": "--per-volume-gigabytes=4G",
            },
            "Cores": {
                "quota_name": OpenstackQuotaName.CORES,
                "value": "15",
                "expected_cli": "--cores=15",
            },
        }
    )
)
def test_OpenstackQuotaEntry_name_to_cli_works(quota_name: OpenstackQuotaName, value: str, expected_cli: str):
    gotten_cli = OpenstackQuotaEntry(name=quota_name, value=value).to_cli()
    assert gotten_cli == expected_cli


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "Gigabytes passing 10G": {
                "human_str": "10G",
                "quota_name": OpenstackQuotaName.GIGABYTES,
                "expected_value": 10,
            },
            "Gigabytes passing 10": {
                "human_str": "10",
                "quota_name": OpenstackQuotaName.GIGABYTES,
                "expected_value": 10,
            },
            "CORES passing 20": {
                "human_str": "20",
                "quota_name": OpenstackQuotaName.CORES,
                "expected_value": 20,
            },
            "RAM passing 20": {
                "human_str": "20",
                "quota_name": OpenstackQuotaName.RAM,
                "expected_value": 20,
            },
            "RAM passing 20M": {
                "human_str": "20M",
                "quota_name": OpenstackQuotaName.RAM,
                "expected_value": 20,
            },
            "RAM passing 20G": {
                "human_str": "20G",
                "quota_name": OpenstackQuotaName.RAM,
                "expected_value": 20 * 1024,
            },
        }
    )
)
def test_OpenstackQuotaEntry___init__works(human_str: str, quota_name: OpenstackQuotaName, expected_value: str):
    gotten_entry = OpenstackQuotaEntry.from_human_spec(human_spec=human_str, name=quota_name)
    assert gotten_entry.value == expected_value


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "Gigabytes passing 10K": {
                "human_str": "10K",
                "quota_name": OpenstackQuotaName.GIGABYTES,
            },
            "Gigabytes passing 10M": {
                "human_str": "10M",
                "quota_name": OpenstackQuotaName.GIGABYTES,
            },
            "RAM passing 20K": {
                "human_str": "20K",
                "quota_name": OpenstackQuotaName.RAM,
            },
        }
    )
)
def test_OpenstackQuotaEntry___init__raises(human_str: str, quota_name: OpenstackQuotaName):
    with pytest.raises(OpenstackBadQuota):
        OpenstackQuotaEntry.from_human_spec(human_spec=human_str, name=quota_name)


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        test_cases={
            "10G RAM + 200M RAM": {
                "quota_name": OpenstackQuotaName.RAM,
                "human_spec1": "10G",
                "human_spec2": "100M",
                "expected_sum": 10340,
            },
            "10G RAM + 200G RAM": {
                "quota_name": OpenstackQuotaName.RAM,
                "human_spec1": "10G",
                "human_spec2": "100G",
                "expected_sum": 10 * 1024 + 100 * 1024,
            },
            "10 RAM + 200G RAM": {
                "quota_name": OpenstackQuotaName.RAM,
                "human_spec1": "10",
                "human_spec2": "100G",
                "expected_sum": 10 + 100 * 1024,
            },
            "10 CORES + 200 CORES": {
                "quota_name": OpenstackQuotaName.CORES,
                "human_spec1": "10",
                "human_spec2": "100",
                "expected_sum": 110,
            },
            "10 Gigabytes + 200G Gigabytes": {
                "quota_name": OpenstackQuotaName.GIGABYTES,
                "human_spec1": "10",
                "human_spec2": "200G",
                "expected_sum": 210,
            },
        }
    )
)
def test_summing_up_two_quota_entries(
    quota_name: OpenstackQuotaName, human_spec1: str, human_spec2: str, expected_sum: int
):
    entry1 = OpenstackQuotaEntry.from_human_spec(name=quota_name, human_spec=human_spec1)
    entry2 = OpenstackQuotaEntry.from_human_spec(name=quota_name, human_spec=human_spec2)
    assert int(entry1.value) + int(entry2.value) == expected_sum


def test_OpenstackAPI_quota_show_happy_path():
    fake_remote = TestUtils.get_fake_remote(
        # openstack quota show -f json admin-monitoring
        responses=[
            """{
                "backup-gigabytes":1000,
                "backups":0,
                "cores":15,
                "fixed-ips":-1,
                "floating-ips":0,
                "gigabytes":80,
                "gigabytes___DEFAULT__":-1,
                "gigabytes_standard":-1,
                "groups":4,
                "injected-file-size":10240,
                "injected-files":5,
                "injected-path-size":255,
                "instances":15,
                "key-pairs":100,
                "location":{
                    "cloud":"",
                    "region_name":"eqiad1-r",
                    "zone":null,
                    "project":{
                        "id":"admin",
                        "name":"admin",
                        "domain_id":"default",
                        "domain_name":"default"
                    }
                },
                "networks":100,
                "per-volume-gigabytes":-1,
                "ports":500,
                "project":"admin-monitoring",
                "project_name":"admin-monitoring",
                "properties":128,
                "ram":32768,
                "rbac_policies":10,
                "routers":10,
                "secgroup-rules":100,
                "secgroups":40,
                "server-group-members":10,
                "server-groups":10,
                "snapshots":4,
                "snapshots___DEFAULT__":-1,
                "snapshots_standard":-1,
                "subnet_pools":-1,
                "subnets":100,
                "volumes":8,
                "volumes___DEFAULT__":-1,
                "volumes_standard":-1
            }"""
        ]
    )
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.CODFW1DEV)
    gotten_quotas = my_api.quota_show()

    fake_remote.query.assert_called_once()
    fake_remote.query.return_value.run_sync.assert_called_once()

    assert OpenstackQuotaName.GIGABYTES in gotten_quotas
    assert gotten_quotas[OpenstackQuotaName.GIGABYTES] == OpenstackQuotaEntry(
        name=OpenstackQuotaName.GIGABYTES, value=80
    )


def test_OpenstackAPI_quota_set_happy_path():
    fake_remote = TestUtils.get_fake_remote(responses=[""])
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.CODFW1DEV)
    my_api.quota_set(
        OpenstackQuotaEntry(name=OpenstackQuotaName.CORES, value=10),
        OpenstackQuotaEntry(name=OpenstackQuotaName.GIGABYTES, value=20),
        OpenstackQuotaEntry(name=OpenstackQuotaName.FLOATING_IPS, value=30),
    )
    expected_command = cumin.transports.Command(
        ("wmcs-openstack quota set --cores=10 --gigabytes=20 --floating-ips=30 admin-monitoring"),
        ok_codes=[0],
    )
    fake_control_host = fake_remote.query.return_value
    fake_control_host.run_sync.assert_called_with(expected_command, is_safe=False)


def test_OpenstackAPI_quota_increase_happy_path():
    fake_remote = TestUtils.get_fake_remote(
        # openstack quota show -f json admin-monitoring
        responses=[
            """{
                "backup-gigabytes":1000,
                "backups":0,
                "cores":15,
                "fixed-ips":-1,
                "floating-ips":0,
                "gigabytes":80,
                "gigabytes___DEFAULT__":-1,
                "gigabytes_standard":-1,
                "groups":4,
                "injected-file-size":10240,
                "injected-files":5,
                "injected-path-size":255,
                "instances":15,
                "key-pairs":100,
                "location":{
                    "cloud":"",
                    "region_name":"eqiad1-r",
                    "zone":null,
                    "project":{
                        "id":"admin",
                        "name":"admin",
                        "domain_id":"default",
                        "domain_name":"default"
                    }
                },
                "networks":100,
                "per-volume-gigabytes":-1,
                "ports":500,
                "project":"admin-monitoring",
                "project_name":"admin-monitoring",
                "properties":128,
                "ram":32768,
                "rbac_policies":10,
                "routers":10,
                "secgroup-rules":100,
                "secgroups":40,
                "server-group-members":10,
                "server-groups":10,
                "snapshots":4,
                "snapshots___DEFAULT__":-1,
                "snapshots_standard":-1,
                "subnet_pools":-1,
                "subnets":100,
                "volumes":8,
                "volumes___DEFAULT__":-1,
                "volumes_standard":-1
            }""",
            """{
                "backup-gigabytes":1000,
                "backups":0,
                "cores":15,
                "fixed-ips":-1,
                "floating-ips":0,
                "gigabytes":80,
                "gigabytes___DEFAULT__":-1,
                "gigabytes_standard":-1,
                "groups":4,
                "injected-file-size":10240,
                "injected-files":5,
                "injected-path-size":255,
                "instances":15,
                "key-pairs":100,
                "location":{
                    "cloud":"",
                    "region_name":"eqiad1-r",
                    "zone":null,
                    "project":{
                        "id":"admin",
                        "name":"admin",
                        "domain_id":"default",
                        "domain_name":"default"
                    }
                },
                "networks":100,
                "per-volume-gigabytes":-1,
                "ports":500,
                "project":"admin-monitoring",
                "project_name":"admin-monitoring",
                "properties":128,
                "ram":32768,
                "rbac_policies":10,
                "routers":10,
                "secgroup-rules":100,
                "secgroups":40,
                "server-group-members":10,
                "server-groups":10,
                "snapshots":4,
                "snapshots___DEFAULT__":-1,
                "snapshots_standard":-1,
                "subnet_pools":-1,
                "subnets":100,
                "volumes":8,
                "volumes___DEFAULT__":-1,
                "volumes_standard":-1
            }""",
            """{
                "backup-gigabytes":1000,
                "backups":0,
                "cores":25,
                "fixed-ips":-1,
                "floating-ips":30,
                "gigabytes":100,
                "gigabytes___DEFAULT__":-1,
                "gigabytes_standard":-1,
                "groups":4,
                "injected-file-size":10240,
                "injected-files":5,
                "injected-path-size":255,
                "instances":15,
                "key-pairs":100,
                "location":{
                    "cloud":"",
                    "region_name":"eqiad1-r",
                    "zone":null,
                    "project":{
                        "id":"admin",
                        "name":"admin",
                        "domain_id":"default",
                        "domain_name":"default"
                    }
                },
                "networks":100,
                "per-volume-gigabytes":-1,
                "ports":500,
                "project":"admin-monitoring",
                "project_name":"admin-monitoring",
                "properties":128,
                "ram":32768,
                "rbac_policies":10,
                "routers":10,
                "secgroup-rules":100,
                "secgroups":40,
                "server-group-members":10,
                "server-groups":10,
                "snapshots":4,
                "snapshots___DEFAULT__":-1,
                "snapshots_standard":-1,
                "subnet_pools":-1,
                "subnets":100,
                "volumes":8,
                "volumes___DEFAULT__":-1,
                "volumes_standard":-1
            }""",
        ]
    )
    my_api = OpenstackAPI(remote=fake_remote, project="admin-monitoring", cluster_name=OpenstackClusterName.CODFW1DEV)
    my_api.quota_increase(
        OpenstackQuotaEntry(name=OpenstackQuotaName.CORES, value=10),
        OpenstackQuotaEntry(name=OpenstackQuotaName.GIGABYTES, value=20),
        OpenstackQuotaEntry(name=OpenstackQuotaName.FLOATING_IPS, value=30),
    )

    expected_show_command = cumin.transports.Command(
        ("wmcs-openstack quota show admin-monitoring -f json"),
        ok_codes=[0],
    )

    expected_set_command = cumin.transports.Command(
        ("wmcs-openstack quota set --cores=25 --gigabytes=100 --floating-ips=30 admin-monitoring"),
        ok_codes=[0],
    )

    fake_control_host = fake_remote.query.return_value
    assert fake_control_host.run_sync.call_count == 3
    calls = [
        mock.call(expected_show_command, is_safe=False),
        mock.call(expected_set_command, is_safe=False),
        mock.call(expected_show_command, is_safe=False),
    ]
    fake_control_host.run_sync.assert_has_calls(calls)
