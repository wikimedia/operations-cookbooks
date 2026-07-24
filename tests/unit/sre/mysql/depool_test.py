"""
Unit tests for sre.mysql.depool
Test using:
tox -e py311-unit -- tests/unit/sre/mysql/depool_test.py -vv
"""

import json
import logging
from argparse import Namespace
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, Mock, patch

import cookbooks.sre.mysql.pool
from cookbooks.sre.mysql.depool import (
    DepoolRunner,
    _check_depooling_last_instance,
    _fetch_instance_connections_count_detailed,
    _fetch_instance_connections_count_wikiusers,
)
from pytest import fixture

log = logging.getLogger()


# # Fixtures


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


@fixture(autouse=True)
def mock_sr():
    with patch("spicerack.Spicerack", autospec=True) as mock_sr:
        dbctl = mock_sr.dbctl()
        dbctl.instance.pool().announce_message = "<<mock dbctl pool announce msg>>"
        dbctl.instance.depool().announce_message = "<<mock dbctl pool announce msg>>"
        dbctl.config.commit().announce_message = "<<mock dbctl config commit announce msg>>"

        mock_sr.admin_reason.return_value.owner = "<<mock owner>>"
        mock_sr.admin_reason.return_value.reason = "<<mock reason>>"

        def z(task, msg, raises=False):
            log.info(f"mock phabricator task_comment '{task}' '{msg}'")

        mock_sr.phabricator.return_value.task_comment.side_effect = z

        yield mock_sr


@fixture(autouse=True)
def m_jget():
    with patch("cookbooks.sre.mysql.pool._jget") as m:
        yield m


# # Tests


@patch("spicerack.mysql.Instance", autospec=True)
def test_fetch_instance_connections_count(mock_i) -> None:
    # what am I really testing?
    mock_i.fetch_one_row.return_value = {"cnt": 33}
    r = _fetch_instance_connections_count_wikiusers(mock_i)
    sql = "SELECT COUNT(*) AS cnt FROM information_schema.processlist WHERE user LIKE '%%wiki%%'"
    mock_i.fetch_one_row.assert_called_with(sql, ())
    assert r == 33


@patch("spicerack.mysql.Instance", autospec=True)
def test_fetch_instance_connections_count_detailed(mock_i) -> None:
    cur = mock.MagicMock()
    mock_i.cursor.return_value.__enter__.return_value = (None, cur)
    cur.execute.return_value = None

    _ = _fetch_instance_connections_count_detailed(mock_i)

    cur.execute.assert_called_once()
    cur.fetchall.assert_called_once()
    mock_i.check_warnings.assert_called_once_with(cur)


def test_last_instance_depool() -> None:
    j = Path("tests/unit/sre/mysql/dbctl_config_get.json").read_text()
    conf = json.loads(j)

    ac = mock.MagicMock()
    cookbooks.sre.mysql.depool.ask_confirmation = ac
    _check_depooling_last_instance(conf, "pc2012", False)
    ac.assert_called()

    ac.reset_mock()
    _check_depooling_last_instance(conf, "pc1221", False)
    ac.assert_not_called()

    _check_depooling_last_instance(conf, "db2173", False)
    ac.assert_called()

    ac.reset_mock()
    _check_depooling_last_instance(conf, "db1248", False)  # vslow, 2 inst
    ac.assert_not_called()

    _check_depooling_last_instance(conf, "db2227", False)  # vslow, 1 inst
    ac.assert_called()


def test_runner_parsercache_depool(mock_sr, m_jget, caplog):
    mi = MagicMock()
    mi.host.hosts = ["pc1015.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "pc1015"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/pc1015":
            return {
                "instances": [
                    {
                        "dc": "eqiad",
                        "fqdn": "pc1015.eqiad.wmnet",
                        "hostname": "pc1015",
                        "instance_group": "parsercache",
                        "instance_name": "pc1015",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "pc5",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": False,
                        "lag": 0.675514,
                        "pooled_value": 1,
                        "role": "master",
                        "kernel_version": None,
                        "uptime_s": 16237312,
                        "uptime_human": "187 days",
                        "tags": ["🎱︎pooled"],
                        "preferred_candidate": False,
                    }
                ]
            }
        assert False, f"Unmocked {url}"

    m_jget.side_effect = jget

    # in a pinch an SRE depools without task id and without setting reason
    args = Namespace(
        operation="depool",
        reason=None,
        task_id=None,
        slow=None,
        fast=None,
        instance="pc1015",
        downtime=None,
    )
    runner = DepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{pc1015.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("pc1015")

    mock_sr.run_cookbook.assert_called_with("sre.mysql.parsercache", ["pc5", "depool"])
    exp = """\
INFO Using parsercache cookbook
INFO The whole 'pc5' section will be depooled
"""
    assert caplog.text == exp


@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_runner_es_depool(m_check_last_instance, mock_sr, m_jget, caplog):
    mi = MagicMock()
    mi.host.hosts = ["es1050.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "es1050"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.generate.return_value = (generate_ret, None)

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/es1050":
            return {
                "instances": [
                    {
                        "dc": "eqiad",
                        "fqdn": "es1050.eqiad.wmnet",
                        "hostname": "es1050",
                        "instance_group": "core",
                        "instance_name": "es1050",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "es1",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "rep",
                        "kernel_version": None,
                        "uptime_s": 5102255,
                        "uptime_human": "59 days",
                        "tags": ["🎱︎pooled"],
                        "preferred_candidate": False,
                    }
                ]
            }
        assert False, f"Unmocked {url}"

    m_jget.side_effect = jget

    # in a pinch an SRE depools without task id and without setting reason
    args = Namespace(
        operation="depool",
        reason=None,
        task_id=None,
        slow=None,
        fast=None,
        instance="es1050",
        nocheck_external_loads=False,
        downtime=None,
    )
    runner = DepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{es1050.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("es1050")

    assert not mock_sr.run_cookbook.called
    exp = """\
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Monitoring number of wikiuser* connections
INFO Connection drain completed
INFO mock phabricator task_comment 'None' 'Completed depooling of es1050 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp
