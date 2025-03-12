"""
Unit tests for sre.mysql.clone

Test using:
tox -e py311-unit -- tests/unit/sre/mysql/clone_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

from unittest import mock
import pytest

from cookbooks.sre.mysql.clone import (
    parse_db_host_fqdn,
    parse_phabricator_task,
    _parse_replication_status,
    _check_if_target_is_already_on_dbctl,
)


def test_parse_phabricator_task():
    assert parse_phabricator_task("T123") == 123
    assert parse_phabricator_task("TT123") == 123  # acceptable typo
    with pytest.raises(AssertionError):
        parse_phabricator_task("X12345")


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
