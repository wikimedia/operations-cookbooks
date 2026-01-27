"""
Unit tests for sre.mysql.clone

Test using:
tox -e py311-unit -- tests/unit/sre/mysql/clone_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

from argparse import Namespace
from pytest import fixture
from unittest import mock
from unittest.mock import MagicMock, patch
import logging
import pytest

# from spicerack.remote import RemoteHosts

from cookbooks.sre.mysql.clone import (
    MInst,
    parse_db_host_fqdn,
    _parse_replication_status,
    _check_if_target_is_already_on_dbctl,
    CloneMySQLRunner,
)

log = logging.getLogger()

# # fixtures


@fixture(autouse=True)
def mock_durable_shell():
    with (patch("cookbooks.sre.mysql.clone.ensure_shell_is_durable", autospec=True),):
        yield


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


# # tests


def test_parse_db_host_fqdn():
    assert parse_db_host_fqdn("db1000.eqiad.wmnet")[0] == "db1000"
    with pytest.raises(AssertionError):
        parse_db_host_fqdn("foo.example.com")
    with pytest.raises(AssertionError):
        parse_db_host_fqdn("db1000.meow.wmnet")
    with pytest.raises(AssertionError):
        parse_db_host_fqdn("db1000.codfw.wmnet")


def test_parse_replication_status():
    blob = """\
*************************** 1. row ***************************
                Slave_IO_State: Waiting for master to send event
                   Master_Host: db1184.eqiad.wmnet
                   Master_User: repl2024
                   Master_Port: 3306
                 Connect_Retry: 60
               Master_Log_File: db1184-bin.008640
           Read_Master_Log_Pos: 219616541
                Relay_Log_File: db1251-relay-bin.000082
                 Relay_Log_Pos: 219616245
         Relay_Master_Log_File: db1184-bin.008640
              Slave_IO_Running: Yes
             Slave_SQL_Running: Yes
               Replicate_Do_DB:
           Replicate_Ignore_DB:
            Replicate_Do_Table:
        Replicate_Ignore_Table:
       Replicate_Wild_Do_Table:
   Replicate_Wild_Ignore_Table:
                    Last_Errno: 0
                    Last_Error:
                  Skip_Counter: 0
           Exec_Master_Log_Pos: 219615945
               Relay_Log_Space: 219617199
               Until_Condition: None
                Until_Log_File:
                 Until_Log_Pos: 0
            Master_SSL_Allowed: Yes
            Master_SSL_CA_File:
            Master_SSL_CA_Path:
               Master_SSL_Cert:
             Master_SSL_Cipher:
                Master_SSL_Key:
         Seconds_Behind_Master: 0
 Master_SSL_Verify_Server_Cert: No
                 Last_IO_Errno: 0
                 Last_IO_Error:
                Last_SQL_Errno: 0
                Last_SQL_Error:
   Replicate_Ignore_Server_Ids:
              Master_Server_Id: 171978826
                Master_SSL_Crl:
            Master_SSL_Crlpath:
                    Using_Gtid: Slave_Pos
                   Gtid_IO_Pos: 180355171-180355171-148310907,180359172-180359172-49702203,171970637-171970637-2116621969,171978826-171978826-1255763081,171970661-171970661-3655324752,171974720-171974720-2572451842,171970745-171970745-4138488048,171978774-171978774-5,180355190-180355190-1378262411,171970572-171970572-3935877275,180367477-180367477-391885106,180363268-180363268-3447080256,0-171970637-5484646134,180357895-180357895-1629141008
       Replicate_Do_Domain_Ids:
   Replicate_Ignore_Domain_Ids:
                 Parallel_Mode: optimistic
                     SQL_Delay: 0
           SQL_Remaining_Delay: NULL
       Slave_SQL_Running_State: Commit
              Slave_DDL_Groups: 3
Slave_Non_Transactional_Groups: 0
    Slave_Transactional_Groups: 73648346
"""
    fn, pos = _parse_replication_status(blob)
    assert (fn, pos) == ("db1184-bin.008640", 219615945)


@mock.patch("spicerack.dbctl.Dbctl", autospec=True)
def test_check_if_target_is_already_on_dbctl(dbctl):
    dbctl.instance.get.return_value = None
    assert not _check_if_target_is_already_on_dbctl(dbctl, "db0000", "s0")

    dbci = mock.MagicMock()
    dbci.sections = {}
    dbctl.instance.get.return_value = dbci
    assert not _check_if_target_is_already_on_dbctl(dbctl, "db0000", "s0")

    # Examples:
    {"s1": {"groups": {"api": {"pooled": True, "weight": 100}}, "percentage": 100, "pooled": True, "weight": 200}}
    {"pc7": {"percentage": 100, "pooled": True, "weight": 1}}
    {"es7": {"percentage": 100, "pooled": True, "weight": 100}}

    dbci.sections = {"s0": {"pooled": False}}
    assert _check_if_target_is_already_on_dbctl(dbctl, "db0000", "s0")


yamlconf = dict(replication_user="ru", replication_password="rp")


@patch("cookbooks.sre.mysql.clone.time.sleep")
@patch("cookbooks.sre.mysql.clone.retry", autospec=True)
@patch("cookbooks.sre.mysql.clone.ask_confirmation", autospec=True)
@patch("cookbooks.sre.mysql.clone._add_host_to_zarcillo", autospec=True)
@patch("cookbooks.sre.mysql.clone.Transferer", autospec=True)
@patch("cookbooks.sre.mysql.clone._run", autospec=True)
@patch("cookbooks.sre.mysql.clone.load_yaml_config", autospec=True, return_value=yamlconf)
@patch("cookbooks.sre.mysql.clone.get_db_instance", autospec=True)
@patch("cookbooks.sre.mysql.clone._remotehosts_query")
@patch("spicerack.Spicerack", autospec=True)
def test_run(
    m_sr,
    m_remotehosts_query,
    m_gdbi,
    m_loadyaml,
    m_run,
    m_xfr,
    m_add_host_zarc,
    m_ask_conf,
    m_retry,
    m_sleep,
    caplog,
):
    m_sr.run_cookbook = mock.Mock()

    def netbox(hn):
        if hn in ["db1001", "db1002", "db1003"]:
            m = MagicMock()
            m.as_dict.return_value = {"site": {"slug": "eqiad"}, "rack": {"name": "RN"}}
            m.status = "active"
            return m

        assert 0, f"Unmocked netbox {hn}"

    m_sr.netbox_server = netbox

    def gdbi(_, fqdn):
        m = MagicMock(spec=MInst)
        if fqdn == "db1002.eqiad.wmnet":  # source
            m.show_slave_status.return_value = dict(Master_Host="db1001.eqiad.wmnet")  # primary
            return m

        elif fqdn == "db1215.eqiad.wmnet":  # zarcillo

            def x(sql, par, database=""):
                assert database == "zarcillo"
                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM instances WHERE server = %s", ("db1003.eqiad.wmnet",)):
                    return dict(cnt=0)

                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM section_instances WHERE instance = %s", ("db1003",)):
                    return dict(cnt=0)

                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s", ("db1003",)):
                    return dict(cnt=0)

                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s", ("db1002",)):
                    return dict(cnt=0)

                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s", ("db1001",)):
                    return dict(cnt=1)

                if (sql, par) == ("SELECT COUNT(*) AS cnt FROM instances WHERE server = %s", ("db1002.eqiad.wmnet",)):
                    return dict(cnt=1)

                if (sql, par) == (
                    "SELECT name, port, `group` FROM instances WHERE server = %s",
                    ("db1002.eqiad.wmnet",),
                ):
                    return dict(name="foo:123", server="", port=3306)

                if (sql, par) == (
                    "SELECT name, port, `group` FROM instances WHERE server = %s",
                    ("db1001.eqiad.wmnet",),
                ):
                    return dict(name="foo:444", server="", port=3306)

                if (sql, par) == ("SELECT section FROM section_instances WHERE instance = %s", "foo:123"):
                    return dict(section="s3")

                if (sql, par) == ("SELECT section FROM section_instances WHERE instance = %s", "foo:444"):
                    return dict(section="s3")

                assert 0, f"Unmocked zarcillo query {sql!r} {par!r}"

            m.fetch_one_row = x
            return m

        if fqdn == "db1003.eqiad.wmnet":  # target
            # m.show_slave_status.return_value = dict(Master_Host="db1001.eqiad.wmnet")  # primary
            return m

        assert 0, f"Unmocked netbox get_db_instance for {fqdn}"

    m_gdbi.side_effect = gdbi

    args = Namespace(
        source="db1002.eqiad.wmnet", target="db1003.eqiad.wmnet", task_id="T0", nopool=True, ignore_existing=False
    )

    # mock sr.dbctl().instance.get(...).sections
    m_sr.dbctl.return_value.instance.get.return_value.sections = {"s3": {"pooled": True, "candidate_master": False}}

    # mock _remotehosts_query
    def mrq(remote, query, fqdn):
        lookup = {"db1002.eqiad.wmnet": "src", "db1003.eqiad.wmnet": "tgt", "db1001.eqiad.wmnet": "pri"}
        kind = lookup[fqdn]
        mock_hosts = MagicMock(name=f"Mock RemoteHosts for {fqdn} as {kind}")
        mock_hosts.hosts = [fqdn]
        mock_hosts.__kind = kind
        return mock_hosts

    m_remotehosts_query.side_effect = mrq

    # mock _run
    def _run(host, cmd, *a, **kw):
        n = host.__kind
        if (n, cmd) == ("src", r'mysql -e "SHOW SLAVE STATUS\G"'):
            return "\nMaster_Log_File: foo\nExec_Master_Log_Pos: 4"
        if n == "src":
            expected = [
                'mysql -e "STOP SLAVE;"',
                "service mariadb stop",
                "systemctl start mariadb",
                "systemctl restart mariadb",
                'mysql -e "START SLAVE;"',
            ]
            if cmd in expected:
                return ""

        if n == "tgt":
            expected = [
                'mysql -e "STOP SLAVE;"',
                "service mariadb stop",  # TODO systemctl
                "rm -rf /srv/sqldata/",
                "chown -R mysql:mysql /srv/*",
                'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
                "systemctl start mariadb",
                'mysql -e "STOP SLAVE; RESET SLAVE ALL"',
                'mysql -e "START SLAVE;"',
                "mysql_upgrade --force",
                "systemctl start mariadb",
            ]
            if cmd in expected:
                return ""

            # TODO
            if "CHANGE MASTER TO" in cmd:
                return ""

        assert 0, f"Unexpected call to _run {host} with params '{cmd}' {a} {kw} that needs mocking"

    m_run.side_effect = _run

    m_xfr.return_value.run.return_value = [0]

    runner = CloneMySQLRunner(args, m_sr)

    runner.run()

    exp = """\
INFO Searching remote 'A:db-all and not A:db-multiinstance and P{db1002.eqiad.wmnet}'
INFO Searching remote 'A:db-all and not A:db-multiinstance and P{db1003.eqiad.wmnet}'
INFO Searching remote 'A:db-all and not A:db-multiinstance and P{db1001.eqiad.wmnet}'
INFO [cookbooks.sre.mysql.clone.check] Running pre-flight checks
INFO [cookbooks.sre.mysql.clone.check] Checking current pooling status
WARNING db1003 is already pooled in according to {'s3': {'pooled': True, 'candidate_master': False}}
INFO [cookbooks.sre.mysql.clone.depool] Depooling source db1002.eqiad.wmnet
INFO [cookbooks.sre.mysql.clone.depool] Depooling target db1003.eqiad.wmnet
INFO [cookbooks.sre.mysql.clone.icinga] Disabling monitoring for source and target host
INFO [cookbooks.sre.mysql.clone.clone] Running the cloning tool
INFO [db1002] Stopping replication using STOP SLAVE
INFO [db1002] Stopping mariadb
INFO [db1003] Stopping replication using STOP SLAVE
INFO [db1003] Stopping mariadb
INFO [db1003] Removing /srv/sqldata
INFO Starting transfer
INFO Transfer complete
INFO [db1002] Starting mariadb
INFO [cookbooks.sre.mysql.clone.conf] [db1003] Configure and start replication
INFO [cookbooks.sre.mysql.clone.catchup_repl_s] [db1002] Catching up replication lag before removing icinga downtime
INFO Replication is healthy
INFO [cookbooks.sre.mysql.clone.wait_icinga_s] [db1002] Waiting for icinga to go green
INFO [cookbooks.sre.mysql.clone.icinga] [db1002] Removing icinga downtime
INFO [cookbooks.sre.mysql.clone.pool] [db1002] Pooling in source host
INFO [cookbooks.sre.mysql.clone.upgrade] [db1003] Run mysql_upgrade then configure GTID
INFO Replication is healthy
INFO [cookbooks.sre.mysql.clone.zarc] [db1003] Adding host to Zarcillo
INFO [cookbooks.sre.mysql.clone.catchup_repl_t] [db1003] Catching up replication lag
INFO Replication is healthy
INFO [cookbooks.sre.mysql.clone.done] Done
"""
    assert caplog.text == exp
