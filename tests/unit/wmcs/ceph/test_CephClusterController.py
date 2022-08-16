import json
from typing import Any, Dict, List, Optional, Type
from unittest import mock

import pytest
from cumin.transports import Command
from spicerack import Spicerack

from cookbooks.wmcs.libs.ceph import (
    CLUSTER_ALERT_MATCHES,
    CephClusterController,
    CephClusterUnhealthy,
    CephFlagSetError,
    CephNoControllerNode,
    CephOSDFlag,
    CephTestUtils,
    CephTimeout,
)
from cookbooks.wmcs.libs.inventory import CephClusterName


def parametrize(params: Dict[str, Any]):
    def decorator(decorated):
        return pytest.mark.parametrize(**CephTestUtils.to_parametrize(params))(decorated)

    return decorator


@parametrize(
    {
        "When there's no nodes, returns empty dict.": {
            "expected_nodes": {},
            "nodes_command_output": "{}",
        },
        "When there's some output (single line), returns the correct dict.": {
            "expected_nodes": {
                "mon": {"monhost1": ["mon1"], "monhost2": ["mon2"]},
                "osd": {"osdhost1": [0, 1], "osdhost2": [2, 3]},
                "mgr": {"mgrhost1": ["mgr1"], "mgrhost2": ["mgr2"]},
            },
            "nodes_command_output": (
                '{"mon":{"monhost1":["mon1"],"monhost2":["mon2"]}, "osd":{"osdhost1":[0,1],"osdhost2":[2,3]}, '
                '"mgr":{"mgrhost1":["mgr1"],"mgrhost2":["mgr2"]}}'
            ),
        },
        "When there's some output (and multiple lines), parses only the last line.": {
            "expected_nodes": {
                "mon": {"monhost1": ["mon1"], "monhost2": ["mon2"]},
                "osd": {"osdhost1": [0, 1], "osdhost2": [2, 3]},
                "mgr": {"mgrhost1": ["mgr1"], "mgrhost2": ["mgr2"]},
            },
            "nodes_command_output": "\n".join(
                [
                    "Some extra output",
                    (
                        '{"mon":{"monhost1":["mon1"],"monhost2":["mon2"]}, "osd":{"osdhost1":[0,1],"osdhost2":[2,3]},'
                        ' "mgr":{"mgrhost1":["mgr1"],"mgrhost2":["mgr2"]}}'
                    ),
                ]
            ),
        },
    }
)
def test_get_nodes_happy_path(expected_nodes: List[str], nodes_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[nodes_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    gotten_nodes = my_controller.get_nodes()

    assert gotten_nodes == expected_nodes


@parametrize(
    {
        "When there's only one other node, returns the other node.": {
            "expected_controlling_node": "monhost2.eqiad.wmnet",
            "nodes_command_output": '{"mon":{"cloudcephmon1001":["mon1"],"monhost2":["mon2"]}}',
        },
    },
)
def test_change_controlling_node_happy_path(expected_controlling_node: str, nodes_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[nodes_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    my_controller.change_controlling_node()

    assert my_controller.controlling_node_fqdn == expected_controlling_node


@parametrize(
    {
        "When there's no other nodes it raises CephNoControllerNode": {
            "nodes_command_output": '{"mon":{"cloudcephmon1001":["mon1"]}}'
        },
    },
)
def test_change_controlling_node_raising(nodes_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[nodes_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    with pytest.raises(CephNoControllerNode):
        my_controller.change_controlling_node()


@parametrize(
    {
        "It generates a status with the correct status dict.": {
            "status_command_output": json.dumps(CephTestUtils.get_status_dict()),
            "expected_status_dict": CephTestUtils.get_status_dict(),
        },
    },
)
def test_get_cluster_status_happy_path(status_command_output: str, expected_status_dict: Dict[str, Any]):
    fake_remote = CephTestUtils.get_fake_remote(responses=[status_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    my_status = my_controller.get_cluster_status()

    assert my_status.status_dict == expected_status_dict


@parametrize(
    {
        "Passes if flag was set (output has the correct format)": {
            "set_flag_command_output": f"{CephOSDFlag.NOREBALANCE.value} is set",
        },
        "Passes if flag was set (output has the correct format with newlines)": {
            "set_flag_command_output": f"\n{CephOSDFlag.NOREBALANCE.value} is set\n",
        },
    },
)
def test_set_osdmap_flag_happy_path(set_flag_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[set_flag_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    my_controller.set_osdmap_flag(flag=CephOSDFlag.NOREBALANCE)

    my_controller._controlling_node.run_sync.assert_called_with(
        Command(f"ceph osd set {CephOSDFlag.NOREBALANCE.value}", ok_codes=[0]), is_safe=False
    )


@parametrize(
    {
        "Raises CephFlagSetError if the set command does not return the correct output": {
            "set_flag_command_output": f"some error happend when setting {CephOSDFlag.NOREBALANCE.value}",
        },
    },
)
def test_set_osdmap_flag_raising(set_flag_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[set_flag_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    with pytest.raises(CephFlagSetError):
        my_controller.set_osdmap_flag(flag=CephOSDFlag.NOREBALANCE)

    my_controller._controlling_node.run_sync.assert_called_with(
        Command(f"ceph osd set {CephOSDFlag.NOREBALANCE.value}", ok_codes=[0]), is_safe=False
    )


@parametrize(
    {
        "Passes if flag was unset (output has the correct format)": {
            "unset_flag_command_output": f"{CephOSDFlag.NOREBALANCE.value} is unset",
        },
        "Passes if flag was unset (output has the correct format, multiline)": {
            "unset_flag_command_output": f"{CephOSDFlag.NOREBALANCE.value} is unset",
        },
    },
)
def test_unset_osdmap_flag_happy_path(unset_flag_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[unset_flag_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    my_controller.unset_osdmap_flag(flag=CephOSDFlag.NOREBALANCE)

    my_controller._controlling_node.run_sync.assert_called_with(
        Command(f"ceph osd unset {CephOSDFlag.NOREBALANCE.value}", ok_codes=[0]), is_safe=False
    )


@parametrize(
    {
        "Raises CephFlagSetError if the unset command does not return the correct output": {
            "unset_flag_command_output": f"some error happened when unsetting {CephOSDFlag.NOREBALANCE.value}",
        },
    },
)
def test_unset_osdmap_flag_raising(unset_flag_command_output: str):
    fake_remote = CephTestUtils.get_fake_remote(responses=[unset_flag_command_output])
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    with pytest.raises(CephFlagSetError):
        my_controller.unset_osdmap_flag(flag=CephOSDFlag.NOREBALANCE)

    my_controller._controlling_node.run_sync.assert_called_with(
        Command(f"ceph osd unset {CephOSDFlag.NOREBALANCE.value}", ok_codes=[0]), is_safe=False
    )


@parametrize(
    {
        "Does nothing if cluster already in maintenance": {
            "commands_output": [
                json.dumps(CephTestUtils.get_maintenance_status_dict()),
                "noout should not try to be set",
                "norebalance should not try to be set",
            ],
        },
        "Passes if cluster healthy": {
            "commands_output": [
                json.dumps(CephTestUtils.get_ok_status_dict()),
                "noout is set",
                "norebalance is set",
            ],
        },
        "Passes if cluster not healthy but force is True": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
                "noout is set",
                "norebalance is set",
            ],
            "force": True,
        },
    },
)
def test_set_maintenance_happy_path(commands_output: List[str], force: Optional[bool]):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    my_controller.set_maintenance(force=bool(force), reason="Doing some tests")


@parametrize(
    {
        "Raises if cluster unhealthy and not force": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
                "noout should not try to be set",
                "norebalance should not try to be set",
            ],
            "force": False,
            "exception": CephClusterUnhealthy,
        },
        "Raises if it failed to set noout": {
            "commands_output": [
                json.dumps(CephTestUtils.get_ok_status_dict()),
                "noout is not set",
                "norebalance is set",
            ],
            "exception": CephFlagSetError,
        },
        "Raises if it failed to set norebalance": {
            "commands_output": [
                json.dumps(CephTestUtils.get_ok_status_dict()),
                "noout is set",
                "norebalance is not set",
            ],
            "exception": CephFlagSetError,
        },
    },
)
def test_set_maintenance_raising(commands_output: List[str], exception: Type[Exception], force: Optional[bool]):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    with pytest.raises(exception):
        my_controller.set_maintenance(force=bool(force), reason="Doing tests")


@parametrize(
    {
        "Does nothing if cluster not in maintenance": {
            "commands_output": [json.dumps(CephTestUtils.get_ok_status_dict())]
            + [json.dumps([])] * len(CLUSTER_ALERT_MATCHES),
        },
        "Passes if cluster in maintenance": {
            "commands_output": [
                json.dumps(CephTestUtils.get_maintenance_status_dict()),
                "noout is unset",
                "norebalance is unset",
            ]
            + [json.dumps([])] * len(CLUSTER_ALERT_MATCHES),
        },
        "Passes if cluster not healthy but force is True": {
            "commands_output": [json.dumps(CephTestUtils.get_warn_status_dict())]
            + [json.dumps([])] * len(CLUSTER_ALERT_MATCHES),
            "force": True,
        },
        "Passes but does not unset flags if cluster unhealthy and force is True": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
            ]
            + [json.dumps([])] * len(CLUSTER_ALERT_MATCHES),
            "force": True,
        },
    },
)
def test_unset_maintenance_happy_path(commands_output: List[str], force: Optional[bool]):
    fake_remote = CephTestUtils.get_fake_remote(responses=commands_output)
    my_controller = CephClusterController(
        remote=fake_remote,
        cluster_name=CephClusterName.EQIAD1,
        spicerack=CephTestUtils.get_fake_spicerack(fake_remote=fake_remote),
    )

    my_controller.unset_maintenance(force=bool(force))


@parametrize(
    {
        "Raises if cluster unhealthy and not force": {
            "commands_output": [json.dumps(CephTestUtils.get_warn_status_dict())],
            "force": False,
            "exception": CephClusterUnhealthy,
        },
        "Raises if cluster only maintenance and it failed to unset noout": {
            "commands_output": [
                json.dumps(CephTestUtils.get_maintenance_status_dict()),
                "noout is set",
                "norebalance is not set",
            ],
            "exception": CephFlagSetError,
        },
        "Raises if it failed to unset norebalance": {
            "commands_output": [
                json.dumps(CephTestUtils.get_maintenance_status_dict()),
                "noout is not set",
                "norebalance is set",
            ],
            "exception": CephFlagSetError,
        },
    },
)
def test_unset_maintenance_raising(commands_output: List[str], exception: Type[Exception], force: Optional[bool]):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    with pytest.raises(exception):
        my_controller.unset_maintenance(force=bool(force))


@parametrize(
    {
        "Passes if no in-progress events": {
            "commands_output": [json.dumps(CephTestUtils.get_status_dict({"progress_events": {}}))],
            "time_ticks": [0],
        },
        "Passes if in-progress events get resolved before timeout": {
            "commands_output": [
                json.dumps(CephTestUtils.get_status_dict({"progress_events": {"some event": {"progress": 0}}})),
                json.dumps(CephTestUtils.get_status_dict({"progress_events": {}})),
            ],
            "time_ticks": [0, 1],
            "timeout_seconds": 100,
        },
    }
)
def test_wait_for_progress_events_happy_path(
    commands_output: List[str],
    time_ticks: List[int],
    timeout_seconds: Optional[int],
):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    with mock.patch("cookbooks.wmcs.libs.common.time.time", side_effect=time_ticks), mock.patch(
        "cookbooks.wmcs.libs.common.time.sleep"
    ):
        if timeout_seconds is not None:
            my_controller.wait_for_in_progress_events(timeout_seconds=timeout_seconds)
        else:
            my_controller.wait_for_in_progress_events()


@parametrize(
    {
        "Raises if timeout reached before no in-progress events": {
            "commands_output": [
                json.dumps(CephTestUtils.get_status_dict({"progress_events": {"some event": {"progress": 0}}})),
                json.dumps(CephTestUtils.get_status_dict({"progress_events": {"some event": {"progress": 0}}})),
            ],
            "time_ticks": [0, 101],
            "timeout_seconds": 100,
        },
    }
)
def test_wait_for_progress_events_raises(
    commands_output: List[str],
    time_ticks: List[int],
    timeout_seconds: int,
):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    with mock.patch("cookbooks.wmcs.libs.common.time.time", side_effect=time_ticks), mock.patch(
        "cookbooks.wmcs.libs.common.time.sleep"
    ), pytest.raises(CephTimeout):
        my_controller.wait_for_in_progress_events(timeout_seconds=timeout_seconds)


@parametrize(
    {
        "Passes if cluster healthy": {
            "commands_output": [json.dumps(CephTestUtils.get_ok_status_dict())],
            "time_ticks": [0],
        },
        "Passes if cluster in maintenance and cosider_maintenance_healthy True": {
            "commands_output": [json.dumps(CephTestUtils.get_maintenance_status_dict())],
            "time_ticks": [0],
            "consider_maintenance_healthy": True,
        },
        "Passes if in-progress events get resolved before timeout": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
                json.dumps(CephTestUtils.get_ok_status_dict()),
            ],
            "time_ticks": [0, 1],
            "timeout_seconds": 100,
        },
    }
)
def test_wait_for_cluster_health_happy_path(
    commands_output: List[str],
    time_ticks: List[int],
    timeout_seconds: Optional[int],
    consider_maintenance_healthy: Optional[bool],
):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    params: Dict[str, Any] = {}
    if consider_maintenance_healthy is not None:
        params["consider_maintenance_healthy"] = consider_maintenance_healthy
    if timeout_seconds is not None:
        params["timeout_seconds"] = timeout_seconds

    with mock.patch("cookbooks.wmcs.libs.common.time.time", side_effect=time_ticks), mock.patch(
        "cookbooks.wmcs.libs.common.time.sleep"
    ):
        my_controller.wait_for_cluster_healthy(**params)


@parametrize(
    {
        "Raises if cluster not healthy before timeout": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
                json.dumps(CephTestUtils.get_warn_status_dict()),
            ],
            "time_ticks": [0, 101],
            "timeout_seconds": 100,
        },
        "Raises if cluster in maintenance and consider_maintenance_healthy is False": {
            "commands_output": [
                json.dumps(CephTestUtils.get_warn_status_dict()),
                json.dumps(CephTestUtils.get_warn_status_dict()),
            ],
            "time_ticks": [0, 101],
            "timeout_seconds": 100,
            "consider_maintenance_healthy": True,
        },
    }
)
def test_wait_for_cluster_health_raises(
    commands_output: List[str],
    time_ticks: List[int],
    timeout_seconds: int,
    consider_maintenance_healthy: Optional[bool],
):
    my_controller = CephClusterController(
        remote=CephTestUtils.get_fake_remote(responses=commands_output),
        cluster_name=CephClusterName.EQIAD1,
        spicerack=mock.MagicMock(spec=Spicerack),
    )

    params: Dict[str, Any] = {"timeout_seconds": timeout_seconds}
    if consider_maintenance_healthy is not None:
        params["consider_maintenance_healthy"] = consider_maintenance_healthy

    with mock.patch("cookbooks.wmcs.libs.common.time.time", side_effect=time_ticks), mock.patch(
        "cookbooks.wmcs.libs.common.time.sleep"
    ), pytest.raises(CephClusterUnhealthy):
        my_controller.wait_for_cluster_healthy(**params)
