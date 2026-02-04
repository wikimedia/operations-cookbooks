"""
Unit tests for sre.mysql.pool
Test using:
tox -e py311-unit -- tests/unit/sre/mysql/pool_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

import json
import logging
from pytest import fixture, raises
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, patch, Mock
from argparse import Namespace


import cookbooks.sre.mysql.pool
from cookbooks.sre.mysql.pool import (
    PoolDepoolRunner,
    _fetch_instance_connections_count_wikiusers,
    _fetch_instance_connections_count_detailed,
    _check_depooling_last_instance,
    _poll_icinga_notification_status,
)

log = logging.getLogger()

# # Fixtures


@fixture(autouse=True)
def mock_durable_shell_and_sleep():
    with (
        patch("cookbooks.sre.mysql.pool.ensure_shell_is_durable", autospec=True),
        patch("cookbooks.sre.mysql.pool.sleep", autospec=True),
    ):
        yield


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

        def z(task, msg):
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
    cookbooks.sre.mysql.pool.ask_confirmation = ac
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


@patch("spicerack.icinga.IcingaHosts", autospec=True)
def test_poll_icinga_notification_status(mock_ihs) -> None:
    s = mock.MagicMock()
    s.notifications_enabled = True
    mock_ihs.get_status.return_value = {"foo": s}
    _poll_icinga_notification_status(mock_ihs, "foo")
    mock_ihs.get_status.assert_called()


def test_runner_init_from_hostname(mock_sr):
    mi = mock.MagicMock()
    mi.host.hosts = ["db1234.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "db1234"

    mrhs = mock.MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    mock_sr.mysql().get_dbs.return_value.list_hosts_instances.return_value = [mi]
    args = Namespace(operation="pool", reason="test", task_id="T0", slow=None, fast=None, instance="db1234")
    PoolDepoolRunner(args, mock_sr)

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db1234.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db1234")


def test_runner_init_from_fqdn(mock_sr):
    mi = mock.MagicMock()
    mi.host.hosts = ["db1000.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "db1000"

    mrhs = mock.MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    args = Namespace(operation="pool", reason="test", task_id="T0", slow=None, fast=None, instance="db1000.eqiad.wmnet")
    PoolDepoolRunner(args, mock_sr)

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db1000.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db1000")


@patch("cookbooks.sre.mysql.pool._jget")
def test_runner_unsupported_section(m_jget, caplog) -> None:

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/db2235":
            return {
                "instances": [
                    {
                        "dc": "codfw",
                        "fqdn": "db2235.codfw.wmnet",
                        "hostname": "db2235",
                        "instance_group": "misc",
                        "instance_name": "db2235",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "m5",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": False,
                        "lag": 1.65583,
                        "pooled_value": None,
                        "role": "",
                        "kernel_version": None,
                        "uptime_s": 14543896,
                        "uptime_human": "168 days",
                        "tags": [],
                        "preferred_candidate": False,
                    }
                ]
            }
        assert False, f"Unmocked {url}"

    m_jget.side_effect = jget

    # m5 not supported
    with raises(RuntimeError):
        ffz = cookbooks.sre.mysql.pool.fetch_host_instance_from_zarcillo
        ffz("db2235")


@patch("cookbooks.sre.mysql.pool._jget")
def test_runner_pc_section(m_jget, caplog) -> None:

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

    # pc5 is supported
    ffz = cookbooks.sre.mysql.pool.fetch_host_instance_from_zarcillo
    ffz("pc1015")


def test_runner_parsercache_pool(mock_sr, m_jget):
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

    args = Namespace(
        operation="pool", reason="test", task_id="T0", slow=None, fast=None, instance="pc1015", skip_safety_checks=False
    )
    runner = PoolDepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{pc1015.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("pc1015")

    mock_sr.run_cookbook.assert_called_with(
        "sre.mysql.parsercache", ["--reason", "test", "--task-id", "T0", "pc5", "pool"]
    )


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
    )
    runner = PoolDepoolRunner(args, mock_sr)
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


@patch("cookbooks.sre.mysql.pool._check_depooling_last_instance", autospec=True)
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
    )
    runner = PoolDepoolRunner(args, mock_sr)
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


def test_runner_s_pool(mock_sr, m_jget, caplog) -> None:
    mi = MagicMock()
    mi.host.hosts = ["db1229.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "db1229"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0)
    mock_sr.dbctl().config.generate.return_value = (generate_ret, None)

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/db1229":
            return {
                "instances": [
                    {
                        "dc": "eqiad",
                        "fqdn": "db1229.eqiad.wmnet",
                        "hostname": "db1229",
                        "instance_group": "core",
                        "instance_name": "db1229",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "s2",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "rep",
                        "kernel_version": None,
                        "uptime_s": 65915,
                        "uptime_human": "18 h",
                        "tags": [
                            "SystemdUnitFailed wmf_auto_restart_prometheus-mysqld-exporter.service on db1229:9100",
                            "🎱︎pooled",
                        ],
                        "preferred_candidate": False,
                    }
                ]
            }

        assert False, f"Unmocked {url}"

    m_jget.side_effect = jget

    args = Namespace(
        operation="pool",
        reason="Ready to pool",
        task_id="T0",
        slow=None,
        fast=None,
        instance="db1229",
        skip_safety_checks=False,
    )
    runner = PoolDepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db1229.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db1229")

    assert not mock_sr.run_cookbook.called

    exp = """\
DEBUG Waiting for icinga to go green
INFO mock phabricator task_comment 'T0' 'Starting pool of db1229 by <<mock owner>>: <<mock reason>>'
INFO Pooling instance db1229 at 6%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db1229 at 25%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db1229 at 56%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db1229 at 100%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
DEBUG pooling-in completed
INFO mock phabricator task_comment 'T0' 'Completed pooling of db1229 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp


def test_runner_x_pool(mock_sr, m_jget, caplog) -> None:
    mi = MagicMock()
    mi.host.hosts = ["db2249.codfw.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "db2249"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0)
    mock_sr.dbctl().config.generate.return_value = (generate_ret, None)

    def jget(url: str) -> dict:
        assert url == "https://zarcillo.wikimedia.org/api/v1/instances/db2249"
        return {
            "instances": [
                {
                    "dc": "codfw",
                    "fqdn": "db2249.codfw.wmnet",
                    "hostname": "db2249",
                    "instance_group": "core",
                    "instance_name": "db2249",
                    "last_start": None,
                    "mariadb_version": None,
                    "port": 3306,
                    "section": "x1",
                    "alerts": [],
                    "candidate_score": 0,
                    "is_candidate_on_dbctl": None,
                    "is_lagging": False,
                    "lag": 0.823505,
                    "pooled_value": None,
                    "role": "",
                    "kernel_version": None,
                    "uptime_s": 578863,
                    "uptime_human": "6 days",
                    "tags": [],
                    "preferred_candidate": False,
                }
            ]
        }

    m_jget.side_effect = jget

    args = Namespace(
        operation="pool",
        reason="Ready to pool",
        task_id="T0",
        slow=None,
        fast=None,
        instance="db2249",
        skip_safety_checks=False,
    )
    runner = PoolDepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db2249.codfw.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db2249")

    assert not mock_sr.run_cookbook.called

    exp = """\
DEBUG Waiting for icinga to go green
INFO mock phabricator task_comment 'T0' 'Starting pool of db2249 by <<mock owner>>: <<mock reason>>'
INFO Pooling instance db2249 at 6%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db2249 at 25%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db2249 at 56%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Pooling instance db2249 at 100%
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
DEBUG pooling-in completed
INFO mock phabricator task_comment 'T0' 'Completed pooling of db2249 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp
