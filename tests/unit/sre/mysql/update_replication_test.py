"""
Unit tests for sre.mysql.update-replication

Test using:
tox -e py311-unit -- tests/unit/sre/mysql/update-replication_test.py -vv
"""

import logging
from unittest.mock import MagicMock

from pytest import fixture
from spicerack.mysql import Instance as MInst

log = logging.getLogger()
log.setLevel(logging.DEBUG)


# Fixtures


@fixture()
def update_repl(load_cookbook):
    return load_cookbook("cookbooks/sre/mysql/update-replication.py")


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


def setup_cb(cb_class, mock_sr, args: list[str]):
    # TODO: reuse in other tests if useful
    cb_instance = cb_class(spicerack=mock_sr)
    ap = cb_instance.argument_parser()
    print("instance", cb_instance)
    print("argument parser ", cb_instance.argument_parser.__doc__)
    print("argument parser ", cb_instance.argument_parser)
    print("argument parser ", cb_instance.argument_parser())
    parsed_args = ap.parse_args(args)
    return cb_instance.get_runner(parsed_args)


def mock_execute(sql, args=()) -> dict:
    sql = sql.strip()
    queries = {
        "STOP REPLICA",
        "CHANGE MASTER TO MASTER_USE_GTID = no",
        """CHANGE MASTER TO
            MASTER_HOST=%(master_fqdn)s,
            MASTER_USER=%(user)s,
            MASTER_PASSWORD=%(password)s,
            MASTER_PORT=%(port)s,
            MASTER_SSL=1""",
        "CHANGE MASTER TO MASTER_USE_GTID = slave_pos",
        "DELETE FROM heartbeat.heartbeat;",
        "START REPLICA",
    }
    if sql in queries:
        return {}
    else:
        raise RuntimeError(f"Unmocked SQL '{sql}'")


# Tests


def test_run_basic_dry_run(mocker, caplog, update_repl):
    mocker.patch.object(update_repl, "ensure_shell_is_durable")
    mocker.patch.object(update_repl.time, "sleep")

    sr = mocker.MagicMock(name="Spicerack")
    cb = setup_cb(
        update_repl.UpdateReplication,
        sr,
        ["--master", "db-test1001", "--replica", "db-test1002", "--port", "3306"],
    )

    mocker.patch.object(cb, "_load_replication_user_password", return_value=("USER", "PASS"))

    def get_minst(name: str):
        m = mocker.MagicMock(name=f"_get_minst for {name}", spec=MInst)
        m.host = MagicMock()
        m.host.__str__.return_value = name + ".eqiad.wmnet"
        m.show_slave_status.return_value = dict(Slave_IO_Running="y", Slave_SQL_Running="y")
        return m

    cb.get_minst = get_minst

    cb.run()

    exp = """\
INFO DRY-RUN: would have executed SQL: <STOP REPLICA>
INFO DRY-RUN: would have executed SQL: <CHANGE MASTER TO MASTER_USE_GTID = no>
INFO DRY-RUN: would have executed SQL: <CHANGE MASTER TO
            MASTER_HOST=%(master_fqdn)s,
            MASTER_USER=%(user)s,
            MASTER_PASSWORD=%(password)s,
            MASTER_PORT=%(port)s,
            MASTER_SSL=1>
INFO DRY-RUN: would have executed SQL: <CHANGE MASTER TO MASTER_USE_GTID = slave_pos>
INFO DRY-RUN: would have executed SQL: <START REPLICA>
"""
    assert caplog.text == exp


def test_run_basic(mocker, caplog, update_repl):

    sr = mocker.MagicMock(name="Spicerack")
    sr.dry_run = False
    cb = setup_cb(
        update_repl.UpdateReplication,
        sr,
        [
            "--master",
            "db-test1001",
            "--replica",
            "db-test1002",
            "--port",
            "3306",
            "--delete-heartbeat",
            "--wait-lag-timeout",
            "20",
        ],
    )

    mocker.patch.object(update_repl, "ensure_shell_is_durable")
    mocker.patch.object(update_repl.time, "sleep")

    mocker.patch.object(cb, "_load_replication_user_password", return_value=("USER", "PASS"))

    def get_minst(name: str):
        m = mocker.MagicMock(name=f"_get_minst for {name}", spec=MInst)
        m.host = MagicMock()
        m.host.__str__.return_value = name + ".eqiad.wmnet"
        m.show_slave_status.return_value = dict(Slave_IO_Running="y", Slave_SQL_Running="y")
        m_cur = MagicMock()

        m_cur.execute.side_effect = mock_execute

        m.cursor.return_value.__enter__.return_value = (MagicMock(), m_cur)

        m.show_slave_status.return_value = {
            "Last_Errno": 0,
            "Last_Error": "",
            "Last_IO_Errno": 0,
            "Last_IO_Error": "",
            "Last_SQL_Errno": 0,
            "Last_SQL_Error": "",
            "Master_Host": "db-test1001.eqiad.wmnet",
            "Master_Port": 3306,
            "Master_User": "USER",
            "Slave_IO_Running": "Yes",
            "Slave_IO_State": "Waiting for master to send event",
            "Slave_SQL_Running_State": "Slave has read all relay log; waiting for more updates",
            "Slave_SQL_Running": "Yes",
            "Using_Gtid": "Slave_Pos",
        }

        m.replication_lag.return_value = 1

        return m

    cb.get_minst = get_minst

    cb.run()

    exp = """\
INFO Executing <STOP REPLICA>
INFO Executing <CHANGE MASTER TO MASTER_USE_GTID = no>
INFO Executing <CHANGE MASTER TO
            MASTER_HOST=%(master_fqdn)s,
            MASTER_USER=%(user)s,
            MASTER_PASSWORD=%(password)s,
            MASTER_PORT=%(port)s,
            MASTER_SSL=1>
INFO Executing <CHANGE MASTER TO MASTER_USE_GTID = slave_pos>
INFO Executing <DELETE FROM heartbeat.heartbeat;>
INFO Executing <START REPLICA>
INFO [1/10] checking replica status
INFO Replica status is OK
INFO Executing <DELETE FROM heartbeat.heartbeat;>
INFO [0] Replication is healthy
"""
    assert caplog.text == exp
