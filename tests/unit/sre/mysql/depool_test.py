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
from pymysql.err import OperationalError
from pytest import (
    fixture,
    raises,
)
from wmflib.interactive import InputError

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
        dbctl.config.commit().announce_message = (
            "<<mock dbctl config commit announce msg>>"
        )
        dbctl.section.set_readonly().announce_message = (
            "<<mock dbctl section set_readonly announce msg>>"
        )
        dbctl.section.set_master().announce_message = (
            "<<mock dbctl set_master announce msg>>"
        )
        dbctl.config.generate().announce_message = (
            "<<mock dbctl generate announce msg>>"
        )

        def mock_set_master(section, dc, master_host):
            ret = MagicMock()
            ret.announce_message = f"<<mock dbctl set_master announce msg for {master_host.name} in {section} in {dc}>>"
            return ret

        dbctl.section.set_master.side_effect = mock_set_master

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


def parse_args(mock_sr, args: list):
    return (
        cookbooks.sre.mysql.depool.Depool(spicerack=mock_sr)
        .argument_parser()
        .parse_args(args)
    )


# # Tests
@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_runner_s_depool(m_last, mock_sr, m_jget, caplog) -> None:
    mi = MagicMock()
    mi.host.hosts = ["db1229.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "db1229"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(
        messages=[], success=True, exit_code=0, announce_message="", name="foo1"
    )
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0, name="foo2")
    generate_ret.announce_message = ""
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

    args = parse_args(
        mock_sr,
        [
            "--reason",
            "Depool",
            "--task-id",
            "T0",
            "db1229",
        ],
    )
    runner = DepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db1229.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db1229")

    assert not mock_sr.run_cookbook.called

    exp = """\
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO Monitoring number of wikiuser* connections
DEBUG Found 1 connection(s), checking count
INFO Connection drain completed
INFO mock phabricator task_comment 'T0' 'Completed depooling of db1229 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp

    with patch(
        "cookbooks.sre.mysql.depool._fetch_instance_connections_count_wikiusers"
    ) as m_fetch:
        m_fetch.side_effect = OperationalError
        with raises(InputError):
            runner.run()


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


# # parsercache # #


def test_depool_parsercache(mock_sr, m_jget, caplog):
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


# # es # #


@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_depool_es_replica(m_check_last_instance, mock_sr, m_jget, caplog):
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
                        "role": "replica",
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
DEBUG Found 1 connection(s), checking count
INFO Connection drain completed
INFO mock phabricator task_comment 'None' 'Completed depool es1 replica es1050 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp


@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_depool_es_readonly_master(m_check_last_instance, mock_sr, m_jget, caplog):
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
                        "role": "master",
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

    def mock_zarc(method: str, path: str) -> dict:
        if path == "/api/v1/section_status/es1":
            j = {
                "name": "es1",
                "groups": ["core"],
                "instance_cnt": 6,
                "error_msgs": [],
                "warn_msgs": [],
                "instances": [
                    {
                        "dc": "eqiad",
                        "fqdn": "es1050.eqiad.wmnet",
                        "hostname": "es1050",
                        "instance_group": "core",
                        "instance_name": "es1050",
                        "last_start": None,
                        "mariadb_version": "10.11.16-MariaDB-log",
                        "port": 3306,
                        "section": "es1",
                        "alerts": [],
                        "candidate_score": 380,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "master",
                        "kernel_version": "6.12.90-1",
                        "uptime_s": None,
                        "uptime_human": "49 days",
                        "tags": ["🎱︎pooled", "older MariaDB"],
                        "preferred_candidate": False,
                    },
                    {
                        "dc": "eqiad",
                        "fqdn": "es1052.eqiad.wmnet",
                        "hostname": "es1052",
                        "instance_group": "core",
                        "instance_name": "es1052",
                        "last_start": None,
                        "mariadb_version": "10.11.16-MariaDB-log",
                        "port": 3306,
                        "section": "es1",
                        "alerts": [],
                        "candidate_score": 280,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "rep",
                        "kernel_version": "6.12.90-2",
                        "uptime_s": None,
                        "uptime_human": "43 days",
                        "tags": ["🎱︎pooled", "older MariaDB"],
                        "preferred_candidate": False,
                    },
                    {
                        "dc": "eqiad",
                        "fqdn": "es1055.eqiad.wmnet",
                        "hostname": "es1055",
                        "instance_group": "core",
                        "instance_name": "es1055",
                        "last_start": None,
                        "mariadb_version": "10.11.16-MariaDB-log",
                        "port": 3306,
                        "section": "es1",
                        "alerts": [],
                        "candidate_score": 780,
                        "is_candidate_on_dbctl": True,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "rep",
                        "kernel_version": "6.12.90-2",
                        "uptime_s": None,
                        "uptime_human": "44 days",
                        "tags": [
                            "🎱︎pooled",
                            "older MariaDB",
                            "⭐preferred",
                            "🛟candidate",
                        ],
                        "preferred_candidate": True,
                    },
                    {
                        "dc": "codfw",
                        "fqdn": "es2053.codfw.wmnet",
                        "hostname": "es2053",
                        "instance_group": "core",
                        "instance_name": "es2053",
                        "last_start": None,
                        "mariadb_version": "10.11.17-MariaDB-log",
                        "port": 3306,
                        "section": "es1",
                        "alerts": [],
                        "candidate_score": 400,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "rep",
                        "kernel_version": "6.12.90-2",
                        "uptime_s": None,
                        "uptime_human": "43 days",
                        "tags": ["🎱︎pooled", "⭐preferred"],
                        "preferred_candidate": True,
                    },
                ],
                "hp": None,
            }
            mock_resp = MagicMock()  # spec=requests.Response)
            mock_resp.status_code = 200
            mock_resp.json.return_value = j
            mock_resp.raise_for_status.return_value = None
            return mock_resp

        assert False, f"Unmocked Zarcillo API {method} path {path}"

    runner._zarcillo_client._client = Mock()
    runner._zarcillo_client._client.request = Mock()
    runner._zarcillo_client._client.request.side_effect = mock_zarc

    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{es1050.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    # assert 0, mock_sr.dbctl.return_value.instance.call_args_list
    # mock_sr.dbctl.return_value.instance.get.assert_called_with("es1050") FIXME

    assert not mock_sr.run_cookbook.called
    exp = """\
DEBUG is_rw_section: False
WARNING The fake-master es1050 in RO section es1 is being depooled
WARNING doing a master/replica switchover
INFO Found es1055 as preferred candidate
INFO <<mock dbctl set_master announce msg for es1050 in es1 in eqiad>>
INFO <<mock dbctl pool announce msg>>
INFO <<mock dbctl config commit announce msg>>
INFO mock phabricator task_comment 'None' 'Completed fake-switchover from es1050 to new master es1055 by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp


@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_depool_es_readwrite_master_in_active_dc(m_check_last_instance, mock_sr, m_jget, caplog):
    mi = MagicMock()
    mi.host.hosts = ["es1035.eqiad.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "es1035"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.generate.return_value = (generate_ret, None)

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/es1035":
            return {
                "instances": [
                    {
                        "dc": "eqiad",
                        "fqdn": "es1035.eqiad.wmnet",
                        "hostname": "es1035",
                        "instance_group": "core",
                        "instance_name": "es1035",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "es7",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "master",
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

    args = Namespace(
        operation="depool",
        reason=None,
        task_id=None,
        slow=None,
        fast=None,
        instance="es1035",
        nocheck_external_loads=False,
        downtime=None,
    )
    runner = DepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{es1035.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("es1035")

    assert not mock_sr.run_cookbook.called
    exp = """\
DEBUG is_rw_section: True
WARNING es1035 is a primary or DC master of es7 (RW): setting whole section as read-only!
INFO <<mock dbctl section set_readonly announce msg>>
DEBUG Setting dbctl es7 in codfw
INFO <<mock dbctl section set_readonly announce msg>>
DEBUG Setting dbctl es7 in eqiad
INFO <<mock dbctl config commit announce msg>>
INFO mock phabricator task_comment 'None' 'Completed setting read-write section es7 as read-only by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp


@patch("cookbooks.sre.mysql.depool._check_depooling_last_instance", autospec=True)
def test_depool_es_readwrite_master_in_standby_dc(m_check_last_instance, mock_sr, m_jget, caplog):
    mi = MagicMock()
    mi.host.hosts = ["es2039.codfw.wmnet"]
    mock_sr.dbctl.return_value.instance.get.return_value.name = "es2039"

    mrhs = MagicMock(name="my_mrhs")
    mrhs.__len__.return_value = 1
    assert len(mrhs) == 1
    mock_sr.mysql().get_dbs.return_value = mrhs

    diff_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.diff.return_value = (diff_ret, None)

    generate_ret = Mock(messages=[], success=True, exit_code=0, announce_message="")
    mock_sr.dbctl().config.generate.return_value = (generate_ret, None)

    def jget(url: str) -> dict:
        if url == "https://zarcillo.wikimedia.org/api/v1/instances/es2039":
            return {
                "instances": [
                    {
                        "dc": "codfw",
                        "fqdn": "es2039.codfw.wmnet",
                        "hostname": "es2039",
                        "instance_group": "core",
                        "instance_name": "es2039",
                        "last_start": None,
                        "mariadb_version": None,
                        "port": 3306,
                        "section": "es7",
                        "alerts": [],
                        "candidate_score": 0,
                        "is_candidate_on_dbctl": None,
                        "is_lagging": None,
                        "lag": None,
                        "pooled_value": 1,
                        "role": "master",
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

    args = Namespace(
        operation="depool",
        reason=None,
        task_id=None,
        slow=None,
        fast=None,
        instance="es2039",
        nocheck_external_loads=False,
        downtime=None,
    )
    runner = DepoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{es2039.codfw.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("es2039")

    assert not mock_sr.run_cookbook.called
    exp = """\
DEBUG is_rw_section: True
WARNING es2039 is a primary or DC master of es7 (RW): setting whole section as read-only!
INFO <<mock dbctl section set_readonly announce msg>>
DEBUG Setting dbctl es7 in codfw
INFO <<mock dbctl section set_readonly announce msg>>
DEBUG Setting dbctl es7 in eqiad
INFO <<mock dbctl config commit announce msg>>
INFO mock phabricator task_comment 'None' 'Completed setting read-write section es7 as read-only by <<mock owner>>: <<mock reason>>'
"""
    assert caplog.text == exp
