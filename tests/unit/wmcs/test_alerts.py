import json
from typing import Any, Dict, List, Optional, Type

import cumin
import pytest

from cookbooks.wmcs.libs.alerts import AlertManager
from cookbooks.wmcs.libs.common import TestUtils


def get_stub_silence(silence_id: str):
    return {
        "id": silence_id,
        "status": {"state": "active"},
        "updatedAt": "2022-07-01T16:08:26.754Z",
        "comment": "ACK! This alert was acknowledged using karma on Tue, 14 Jun 2022 08:29:09 GMT",
        "createdBy": "some_user",
        "endsAt": "2022-07-31T16:19:00.000Z",
        "matchers": [{"isRegex": False, "name": "team", "value": "wmcs"}],
        "startsAt": "2022-07-01T16:08:26.754Z",
    }


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "One element query": {
                "query": "alertname='My alert'",
                "expected_command": "amtool --output=json silence query alertname='My alert'",
                "commands_outputs": [
                    json.dumps([get_stub_silence(silence_id="some-silence-id")]),
                ],
                "expected_silences": [get_stub_silence(silence_id="some-silence-id")],
            },
            "Many elements query": {
                "query": "alertname='My alert' service='.*ceph.*'",
                "expected_command": "amtool --output=json silence query alertname='My alert' service='.*ceph.*'",
                "commands_outputs": [
                    json.dumps([get_stub_silence(silence_id="some-silence-id")]),
                ],
                "expected_silences": [get_stub_silence(silence_id="some-silence-id")],
            },
            "Many results": {
                "query": "alertname='My alert'",
                "expected_command": "amtool --output=json silence query alertname='My alert'",
                "commands_outputs": [
                    json.dumps(
                        [
                            get_stub_silence(silence_id="some-silence-id1"),
                            get_stub_silence(silence_id="some-silence-id2"),
                        ]
                    ),
                ],
                "expected_silences": [
                    get_stub_silence(silence_id="some-silence-id1"),
                    get_stub_silence(silence_id="some-silence-id2"),
                ],
            },
        }
    )
)
def test_AlertManager_get_silences_happy_path(
    query: str, expected_command: str, commands_outputs: List[str], expected_silences: List[str]
):
    fake_remote = TestUtils.get_fake_remote(responses=commands_outputs)
    my_alertmanager = AlertManager.from_remote(fake_remote)

    gotten_silences = my_alertmanager.get_silences(query=query)

    assert gotten_silences == expected_silences
    fake_remote.query.return_value.run_sync.assert_called_with(cumin.transports.Command(expected_command, ok_codes=[0]))


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "Just alert_name and comment": {
                "params": {
                    "alert_name": "MyAlert",
                    "comment": "Dummy comment",
                },
                "expected_command": (
                    "amtool --output=json silence add --duration=\"1h\" --comment='Dummy comment' "
                    "alertname='MyAlert'"
                ),
                "commands_outputs": [
                    "some-silence-id",
                ],
                "expected_silence": "some-silence-id",
            },
            "With extra queries": {
                "params": {
                    "alert_name": "MyAlert",
                    "comment": "Dummy comment",
                    "extra_queries": ["service=.*ceph.*", "instance=~cloud.*"],
                },
                "expected_command": (
                    "amtool --output=json silence add --duration=\"1h\" --comment='Dummy comment' "
                    "alertname='MyAlert' 'service=.*ceph.*' 'instance=~cloud.*'"
                ),
                "commands_outputs": [
                    "some-silence-id",
                ],
                "expected_silence": "some-silence-id",
            },
            "With custom duration queries": {
                "params": {
                    "alert_name": "MyAlert",
                    "comment": "Dummy comment",
                    "duration": "8h",
                },
                "expected_command": (
                    "amtool --output=json silence add --duration=\"8h\" --comment='Dummy comment' "
                    "alertname='MyAlert'"
                ),
                "commands_outputs": [
                    "some-silence-id",
                ],
                "expected_silence": "some-silence-id",
            },
        }
    )
)
def test_AlertManager_downtime_alert_happy_path(
    params: Dict[str, Any], expected_command: str, commands_outputs: List[str], expected_silence: str
):
    fake_remote = TestUtils.get_fake_remote(responses=commands_outputs)
    my_alertmanager = AlertManager.from_remote(fake_remote)

    gotten_silence = my_alertmanager.downtime_alert(**params)

    assert gotten_silence == expected_silence
    fake_remote.query.return_value.run_sync.assert_called_with(cumin.transports.Command(expected_command, ok_codes=[0]))


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "If there's no existing silences, does nothing": {
                "params": {
                    "alert_name": "MyAlert",
                },
                "expected_command": None,
                "commands_outputs": [
                    json.dumps([]),
                ],
            },
            "Only alert name": {
                "params": {
                    "alert_name": "MyAlert",
                },
                "expected_command": "amtool --output=json silence expire some-silence-id",
                "commands_outputs": [
                    json.dumps([get_stub_silence("some-silence-id")]),
                ],
            },
            "Only extra_queries": {
                "params": {
                    "extra_queries": ["service=~.*ceph.*", "instance=~cloud.*"],
                },
                "expected_command": "amtool --output=json silence expire some-silence-id1 some-silence-id2",
                "commands_outputs": [
                    json.dumps([get_stub_silence("some-silence-id1"), get_stub_silence("some-silence-id2")]),
                ],
            },
        }
    )
)
def test_AlertManager_uptime_alert_happy_path(
    params: Dict[str, Any], expected_command: Optional[str], commands_outputs: List[str]
):
    fake_remote = TestUtils.get_fake_remote(responses=commands_outputs)
    my_alertmanager = AlertManager.from_remote(fake_remote)

    my_alertmanager.uptime_alert(**params)

    if expected_command is not None:
        fake_remote.query.return_value.run_sync.assert_called_with(
            cumin.transports.Command(expected_command, ok_codes=[0])
        )
    else:
        fake_remote.query.return_value.run_sync.assert_called_once()


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "ValueError If there's no alert_name or extra_queries": {
                "params": {},
                "expected_exception": ValueError,
            },
        }
    )
)
def test_AlertManager_uptime_alert_raises(
    params: Dict[str, Any],
    expected_exception: Type[Exception],
):
    fake_remote = TestUtils.get_fake_remote()
    my_alertmanager = AlertManager.from_remote(fake_remote)

    with pytest.raises(expected_exception):
        my_alertmanager.uptime_alert(**params)


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "Only host_name and comment": {
                "params": {
                    "host_name": "dummy_host",
                    "comment": "Some comment",
                },
                "expected_command": (
                    'amtool --output=json silence add --duration="1h" '
                    "--comment='Some comment' instance=~'dummy_host(:[0-9]+)?'"
                ),
                "commands_outputs": ["some-silence-id"],
                "expected_silence_id": "some-silence-id",
            },
            "Setting custom duration": {
                "params": {
                    "host_name": "dummy_host",
                    "comment": "Some comment",
                    "duration": "8h",
                },
                "expected_command": (
                    'amtool --output=json silence add --duration="8h" '
                    "--comment='Some comment' 'instance=~dummy_host(:[0-9]+)?'"
                ),
                "commands_outputs": ["some-silence-id"],
                "expected_silence_id": "some-silence-id",
            },
        }
    )
)
def test_AlertManager_downtime_host_happy_path(
    params: Dict[str, Any], expected_command: str, commands_outputs: List[str], expected_silence_id: str
):
    fake_remote = TestUtils.get_fake_remote(responses=commands_outputs)
    my_alertmanager = AlertManager.from_remote(fake_remote)

    gotten_silence_id = my_alertmanager.downtime_host(**params)

    assert gotten_silence_id == expected_silence_id
    fake_remote.query.return_value.run_sync.assert_called_with(cumin.transports.Command(expected_command, ok_codes=[0]))


@pytest.mark.parametrize(
    **TestUtils.to_parametrize(
        {
            "If there are no matches does nothing": {
                "params": {
                    "host_name": "dummy_host",
                },
                "expected_command": None,
                "commands_outputs": [json.dumps([])],
            },
            "One matching silence": {
                "params": {
                    "host_name": "dummy_hoste",
                },
                "expected_command": "amtool --output=json silence expire some-silence-id",
                "commands_outputs": [
                    json.dumps([get_stub_silence("some-silence-id")]),
                ],
            },
            "Many matching silences": {
                "params": {
                    "host_name": "dummy_hoste",
                },
                "expected_command": "amtool --output=json silence expire some-silence-id1 some-silence-id2",
                "commands_outputs": [
                    json.dumps([get_stub_silence("some-silence-id1"), get_stub_silence("some-silence-id2")]),
                ],
            },
        }
    )
)
def test_AlertManager_uptime_host_happy_path(
    params: Dict[str, Any], expected_command: str, commands_outputs: List[str]
):
    fake_remote = TestUtils.get_fake_remote(responses=commands_outputs)
    my_alertmanager = AlertManager.from_remote(fake_remote)

    my_alertmanager.uptime_host(**params)

    if expected_command is not None:
        fake_remote.query.return_value.run_sync.assert_called_with(
            cumin.transports.Command(expected_command, ok_codes=[0])
        )
    else:
        fake_remote.query.return_value.run_sync.assert_called_once()
