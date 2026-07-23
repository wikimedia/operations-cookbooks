"""
Unit tests for sre.mysql.pool
Test using:
tox -e py311-unit -- tests/unit/sre/mysql/pool_test.py -vv
"""

import logging
from pytest import fixture, raises
from unittest import mock
from unittest.mock import MagicMock, patch, Mock


import cookbooks.sre.mysql.pool
from cookbooks.sre.mysql.pool import (
    PoolRunner,
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
        dbctl.config.generate().announce_message = "<<mock dbctl generate announce msg>>"

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
    return cookbooks.sre.mysql.pool.Pool(spicerack=mock_sr).argument_parser().parse_args(args)


# # Tests


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
    args = parse_args(
        mock_sr,
        [
            "--reason",
            "test",
            "--task-id",
            "T0",
            "db1234",
        ],
    )
    PoolRunner(args, mock_sr)

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

    args = parse_args(
        mock_sr,
        [
            "--reason",
            "test",
            "--task-id",
            "T0",
            "db1000.eqiad.wmnet",
        ],
    )
    PoolRunner(args, mock_sr)

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

    args = parse_args(
        mock_sr,
        [
            "--reason",
            "test",
            "--task-id",
            "T0",
            "pc1015",
        ],
    )
    runner = PoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{pc1015.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("pc1015")

    mock_sr.run_cookbook.assert_called_with(
        "sre.mysql.parsercache", ["--reason", "test", "--task-id", "T0", "pc5", "pool"]
    )


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

    args = parse_args(
        mock_sr,
        [
            "--reason",
            "Ready to pool",
            "--task-id",
            "T0",
            "db1229",
        ],
    )
    runner = PoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db1229.eqiad.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db1229")

    assert not mock_sr.run_cookbook.called

    exp = """\
DEBUG Waiting for icinga to go green
INFO Removing downtime ahead of pooling
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

    args = parse_args(
        mock_sr,
        [
            "--reason",
            "Ready to pool",
            "--task-id",
            "T0",
            "db2249",
        ],
    )
    runner = PoolRunner(args, mock_sr)
    runner.run()

    mock_sr.mysql.return_value.get_dbs.assert_called_with(
        "P{db2249.codfw.wmnet} and A:db-all and not A:db-multiinstance"
    )
    mock_sr.dbctl.return_value.instance.get.assert_called_with("db2249")

    assert not mock_sr.run_cookbook.called

    exp = """\
DEBUG Waiting for icinga to go green
INFO Removing downtime ahead of pooling
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
