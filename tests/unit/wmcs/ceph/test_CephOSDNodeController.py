import json
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

from cookbooks.wmcs.libs.ceph import CephMalformedInfo, CephOSDNodeController, CephTestUtils


def parametrize(params: Dict[str, Any]):
    def decorator(decorated):
        return pytest.mark.parametrize(**CephTestUtils.to_parametrize(params))(decorated)

    return decorator


AVAILABLE_DEVICE_JSON = json.dumps(CephTestUtils.get_available_device())
AVAILABLE_DEVICE_PATH = f"/dev/{CephTestUtils.get_available_device()['name']}"
SYSTEM_DEVICE_JSON = json.dumps(CephTestUtils.get_available_device(name=CephOSDNodeController.SYSTEM_DEVICES[0]))
NON_DISK_DEVICE_JSON = json.dumps(CephTestUtils.get_available_device(device_type="non-disk"))
MOUNTED_DEVICE_JSON = json.dumps(CephTestUtils.get_available_device(mountpoint="/some/where"))
FORMATTED_DEVICE_JSON = json.dumps(CephTestUtils.get_available_device(children=["child1"]))


@parametrize(
    {
        "When there's no devices, returns empty list.": {
            "expected_devices": [],
            "lsblk_command_output": '{"blockdevices": []}',
        },
        "When there's a device , returns it.": {
            "expected_devices": [AVAILABLE_DEVICE_PATH],
            "lsblk_command_output": f'{{"blockdevices": [{AVAILABLE_DEVICE_JSON}]}}',
        },
        "Skips system devices.": {
            "expected_devices": [AVAILABLE_DEVICE_PATH],
            "lsblk_command_output": f'{{"blockdevices": [{SYSTEM_DEVICE_JSON}, {AVAILABLE_DEVICE_JSON}]}}',
        },
        "Skips non-disk devices.": {
            "expected_devices": [AVAILABLE_DEVICE_PATH],
            "lsblk_command_output": f'{{"blockdevices": [{NON_DISK_DEVICE_JSON}, {AVAILABLE_DEVICE_JSON}]}}',
        },
        "Skips mounted devices.": {
            "expected_devices": [AVAILABLE_DEVICE_PATH],
            "lsblk_command_output": f'{{"blockdevices": [{MOUNTED_DEVICE_JSON}, {AVAILABLE_DEVICE_JSON}]}}',
        },
        "Skips formatted devices (that have children).": {
            "expected_devices": [AVAILABLE_DEVICE_PATH],
            "lsblk_command_output": f'{{"blockdevices": [{FORMATTED_DEVICE_JSON}, {AVAILABLE_DEVICE_JSON}]}}',
        },
    },
)
def test_get_available_devices_happy_path(expected_devices: List[str], lsblk_command_output: str):
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(responses=[lsblk_command_output]),
        node_fqdn="my-osd-fq.dn",
    )

    gotten_devices = my_controller.get_available_devices()

    assert gotten_devices == expected_devices


@parametrize(
    {
        "Raise CephMalformedInfo when there's no blockdevices entry in the lsblk output": {
            "lsblk_command_output": "{}",
        },
    }
)
def test_get_available_devices_raises(lsblk_command_output: str):
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(responses=[lsblk_command_output]),
        node_fqdn="my-osd-fq.dn",
    )

    with pytest.raises(CephMalformedInfo):
        my_controller.get_available_devices()


def test_zap_device_happy_path_does_not_raise():
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(responses=[""]),
        node_fqdn="my-osd-fq.dn",
    )

    my_controller.zap_device(device_path="/dummy/device")


def test_zap_device_happy_path_raises_when_command_fails():
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(side_effect=[Exception]),
        node_fqdn="my-osd-fq.dn",
    )

    with pytest.raises(Exception):
        my_controller.zap_device(device_path="/dummy/device")


def test_initialize_and_start_osd_happy_path_does_not_raise():
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(responses=[""]),
        node_fqdn="my-osd-fq.dn",
    )

    my_controller.initialize_and_start_osd(device_path="/dummy/device")


def test_initialize_and_start_osd_happy_path_raises_when_command_fails():
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(side_effect=[Exception]),
        node_fqdn="my-osd-fq.dn",
    )

    with pytest.raises(Exception):
        my_controller.initialize_and_start_osd(device_path="/dummy/device")


@parametrize(
    {
        "Does nothing if there's no devices, non_interactive": {
            "lsblk_command_output": '{"blockdevices": []}',
            "interactive": False,
        },
        "Does nothing if there's no devices, interactive": {
            "lsblk_command_output": '{"blockdevices": []}',
            "interactive": True,
        },
    }
)
def test_add_all_available_devices_happy_path(lsblk_command_output: str, interactive: bool):
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(responses=[lsblk_command_output]),
        node_fqdn="my-osd-fq.dn",
    )

    my_controller.add_all_available_devices(interactive=interactive)


@patch("cookbooks.wmcs.libs.ceph.ask_confirmation")
def test_add_all_available_devices_asks_confirmation_if_interaciteve_is_True_and_theres_available_devices(
    mock_ask_confirmation,
):
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(
            responses=[
                f'{{"blockdevices": [{AVAILABLE_DEVICE_JSON}]}}',
                "",
                "",
            ]
        ),
        node_fqdn="my-osd-fq.dn",
    )

    my_controller.add_all_available_devices(interactive=True)

    mock_ask_confirmation.assert_called()


@patch("cookbooks.wmcs.libs.ceph.ask_confirmation")
def test_add_all_available_devices_does_not_ask_confirmation_if_interaciteve_is_False_and_theres_available_devices(
    mock_ask_confirmation,
):
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(
            responses=[
                f'{{"blockdevices": [{AVAILABLE_DEVICE_JSON}]}}',
                "",
                "",
            ]
        ),
        node_fqdn="my-osd-fq.dn",
    )

    my_controller.add_all_available_devices(interactive=False)

    mock_ask_confirmation.assert_not_called()


def test_add_all_available_devices_handles_all_devices():
    my_controller = CephOSDNodeController(
        remote=CephTestUtils.get_fake_remote(
            responses=[
                f'{{"blockdevices": [{AVAILABLE_DEVICE_JSON}, {AVAILABLE_DEVICE_JSON}]}}',
                "",
                "",
            ]
        ),
        node_fqdn="my-osd-fq.dn",
    )
    my_controller.zap_device = MagicMock(spec=my_controller.zap_device)
    my_controller.initialize_and_start_osd = MagicMock(my_controller.initialize_and_start_osd)

    my_controller.add_all_available_devices(interactive=False)

    my_controller.zap_device.assert_called()
    assert my_controller.zap_device.call_count == 2
    my_controller.initialize_and_start_osd.assert_called()
    assert my_controller.initialize_and_start_osd.call_count == 2
