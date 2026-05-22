import logging
from unittest.mock import MagicMock

import pytest
from cookbooks.sre.mysql import Phabricator
from cookbooks.sre.mysql.decommission import Decommission, DecommissionRunner
from pytest import fixture

log = logging.getLogger()


def run_cb(cb_class, runner_class, mock_sr, args: list[str]) -> None:
    # TODO: reuse in other tests if useful
    args = cb_class(spicerack=mock_sr).argument_parser().parse_args(args)
    runner = runner_class(args, mock_sr)
    runner.run()


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


@fixture(autouse=True)
def patch_shell_durable(mocker):
    mocker.patch("cookbooks.sre.mysql.decommission.ensure_shell_is_durable")


def test_run_full(mocker, caplog):
    mocker.patch("cookbooks.sre.mysql.decommission.phabricator_client")
    m_ask = mocker.patch("cookbooks.sre.mysql.decommission.ask_confirmation")
    mocker.patch("cookbooks.sre.mysql.decommission._delete_from_zarcillo")
    mocker.patch("cookbooks.sre.mysql.decommission.requests")

    mock_sr = MagicMock(dry_run=False)
    mock_client = mock_sr.api_client.return_value
    m_resp = MagicMock()
    m_resp.json.return_value = {
        "instances": [
            {
                "candidate_score": 0,
                "dc": "eqiad",
                "fqdn": "db1176.eqiad.wmnet",
                "hostname": "db1176",
                "instance_group": "s1",
                "instance_name": "db1176",
                "is_candidate_on_dbctl": True,
                "is_lagging": False,
                "kernel_version": "...",
                "lag": 0,
                "last_start": "...",
                "mariadb_version": "...",
                "pooled_value": 1,
                "port": 3306,
                "preferred_candidate": False,
                "role": "replica",
                "section": "s1",
                "uptime_human": "",
                "uptime_s": 86400,
            }
        ]
    }
    mock_client.request.return_value = m_resp

    m_ask.side_effect = lambda m: log.debug(f"Mock ask '{m}'")

    # return a value on first 2 calls, then None as the db is deleted from dbctl
    mock_sr.dbctl.return_value.instance.get.side_effect = [
        MagicMock(),
        MagicMock(),
        None,
    ]

    run_cb(Decommission, DecommissionRunner, mock_sr, ["db1176", "-t", "T12345"])

    exp = """\
INFO instances on db1176:
INFO     role: replica port: 3306
INFO [cookbooks.sre.mysql.decommission.depool] depooling host if needed
INFO ▶ Create a puppet patch to remove db1176 from dbctl
INFO ▶ example: https://gerrit.wikimedia.org/r/c/operations/puppet/+/638343
INFO ▶ Review and merge and then run:
INFO ▶ ssh puppetserver1001.eqiad.wmnet -t sudo -i puppet-merge
DEBUG Mock ask 'Have you puppet-merged?'
INFO ▶ Then run:
INFO ▶ sudo dbctl config commit -m 'Remove db1176 from dbctl T12345'
DEBUG Mock ask 'Done?'
WARNING db1176 is still showing in dbctl: review manual steps.
DEBUG Mock ask 'Check again?'
INFO [cookbooks.sre.mysql.decommission.run_decommission] Running decommission cookbook
INFO ▶ Create a puppet patch to remove db1176 from Puppet entirely
INFO ▶ example: https://gerrit.wikimedia.org/r/c/operations/puppet/+/1286167
INFO ▶ Review and merge and then run:
INFO ▶ ssh puppetserver1001.eqiad.wmnet -t sudo -i puppet-merge
DEBUG Mock ask 'Done?'
INFO [cookbooks.sre.mysql.decommission.zarcillo_cleanup] Removing host from zarcillo
INFO [cookbooks.sre.mysql.decommission.orchestrator] Running orchestrator cleanup
DEBUG Running on orchestrator: orchestrator -c forget -i db1176:3306
INFO [cookbooks.sre.mysql.decommission.phabricator] Update phabricator task
INFO --------------------------------------------------------------------------------
INFO [cookbooks.sre.mysql.decommission.handover] Update the task and send it to dcops
INFO Phabricator task updated
"""
    assert caplog.text == exp


def test_fetch_instances_refuse_master(mocker):
    from cookbooks.sre.mysql.decommission import _fetch_instances

    mock_sr = MagicMock(dry_run=False)
    mock_client = mock_sr.api_client.return_value
    m_resp = MagicMock()
    m_resp.json.return_value = {
        "instances": [
            {
                "dc": "eqiad",
                "fqdn": "db1176.eqiad.wmnet",
                "hostname": "db1176",
                "instance_group": "s1",
                "instance_name": "db1176",
                "last_start": "...",
                "mariadb_version": "...",
                "port": 3306,
                "section": "s1",
                "candidate_score": 0,
                "is_candidate_on_dbctl": True,
                "is_lagging": False,
                "lag": 0,
                "pooled_value": 1,
                "role": "master",
                "kernel_version": "...",
                "uptime_s": 86400,
                "uptime_human": "1d 0h 0m",
                "preferred_candidate": False,
            }
        ]
    }

    mock_client.request.return_value = m_resp

    with pytest.raises(SystemExit) as exit_info:
        _fetch_instances(mock_sr, "db1176")

    assert exit_info.value.code == 1


def test_update_phab_desc_tick_boxes():
    desc = """\

db1177

**Steps for service owner:**

[] - all system services confirmed offline from production use
[] - set all icinga checks to maint mode/disabled while reclaim/decommmission takes place. (likely done by script)
[] - remove system from all lvs/pybal active configuration
[] - any service group puppet/hiera/dsh config removed
[] - login to cumin host and run the decom cookbook: cookbook sre.hosts.decommission <host fqdn> -t <phab task>.  This does: bootloader wipe, host power down, netbox update to decommissioning status, puppet node clean, puppet node deactivate, debmonitor removal, and run homer.
[] - remove all remaining puppet references and all host entries in the puppet repo
[] - reassign task from service owner to no owner and ensure the site project (ops-sitename depending on site of server) is assigned.

**End service owner steps / Begin DC-Ops team steps:**

[] - system disks removed (by onsite)
[] - determine system age, under 5 years are reclaimed to spare, over 5 years are decommissioned.
[] - IF DECOM: system unracked and decommissioned (by onsite), update netbox with result and set state to offline
[] - IF DECOM: mgmt dns entries removed.
[] - IF RECLAIM: set netbox state to 'inventory' and hostname to asset tag"""
    exp = """\

db1177

**Steps for service owner:**

[x] - all system services confirmed offline from production use
[x] - set all icinga checks to maint mode/disabled while reclaim/decommmission takes place. (likely done by script)
[x] - remove system from all lvs/pybal active configuration
[x] - any service group puppet/hiera/dsh config removed
[x] - login to cumin host and run the decom cookbook: cookbook sre.hosts.decommission <host fqdn> -t <phab task>.  This does: bootloader wipe, host power down, netbox update to decommissioning status, puppet node clean, puppet node deactivate, debmonitor removal, and run homer.
[x] - remove all remaining puppet references and all host entries in the puppet repo
[x] - reassign task from service owner to no owner and ensure the site project (ops-sitename depending on site of server) is assigned.

**End service owner steps / Begin DC-Ops team steps:**

[] - system disks removed (by onsite)
[] - determine system age, under 5 years are reclaimed to spare, over 5 years are decommissioned.
[] - IF DECOM: system unracked and decommissioned (by onsite), update netbox with result and set state to offline
[] - IF DECOM: mgmt dns entries removed.
[] - IF RECLAIM: set netbox state to 'inventory' and hostname to asset tag"""
    out = DecommissionRunner.update_phab_desc_tick_boxes(desc)
    assert out == exp


def test_handover_task_to_dcops_internal(mocker, caplog):
    mock_sr = MagicMock(dry_run=False)
    phab = Phabricator(mock_sr)
    phab._client.maniphest.search.return_value = {
        "data": [
            {
                "phid": "PHID-TASK-1234",
                "id": "1234",
                "fields": {"description": {"raw": "Steps for service owner:\n[] - step 1\n"}},
            }
        ]
    }
    t = phab.fetch_task("T1234")
    t.unassign()
    assert len(t._pending_txns)

    t.send()
    assert not t._pending_txns


def test_handover_task_to_dcops_dryrun(mocker, caplog):
    mock_sr = MagicMock(dry_run=True)
    mock_phab = Phabricator(mock_sr)
    mock_phab._client.maniphest.search.return_value = {
        "data": [
            {
                "phid": "PHID-TASK-1234",
                "id": "111",
                "fields": {"description": {"raw": "Steps for service owner:\n[] - step 1\n"}},
            }
        ]
    }
    mock_phab._client.project.search.return_value = {
        "data": [
            {"phid": "PHID-PROJ-dcops"},
            {"phid": "PHID-PROJ-opseqiad"},
        ]
    }

    dr = DecommissionRunner([], mock_sr)
    dr.phab = mock_phab
    dr.handover_task_to_dcops("T1234", "eqiad")

    mock_phab._client.maniphest.search.assert_called_once_with(constraints={"ids": [1234]})
    mock_phab._client.project.search.assert_called_once_with(constraints={"slugs": ["dc-ops", "ops-eqiad", "DBA"]})

    exp = """\
INFO DRY-RUN: not running phabricator transaction: [{'type': 'owner', 'value': None}, {'type': 'projects.set', 'value': ['PHID-PROJ-dcops', 'PHID-PROJ-opseqiad']}, {'type': 'comment', 'value': 'This host is ready for DC-Ops to decommission'}, {'type': 'description', 'value': 'Steps for service owner:\\n[x] - step 1'}]
"""
    assert caplog.text == exp
