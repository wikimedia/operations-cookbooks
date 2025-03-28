# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

import logging
from argparse import Namespace
from unittest.mock import patch, MagicMock

from spicerack.mysql import MysqlRemoteHosts, Instance as MInst
from spicerack.remote import RemoteHosts

from cookbooks.sre.mysql.sanitarium_restart import run


@patch("cookbooks.sre.mysql.sanitarium_restart.sleep", autospec=True)
@patch("cookbooks.sre.mysql.sanitarium_restart.ensure_shell_is_durable", autospec=True)
@patch("cookbooks.sre.mysql.sanitarium_restart.ask_confirmation", autospec=True)
@patch("spicerack.Spicerack", autospec=True)
def test_run(m_sr, m_ask, m_durab, m_sleep, caplog) -> None:
    caplog.set_level(logging.INFO)
    myhost = MagicMock(spec=MysqlRemoteHosts, name="myhost")
    myhost.__str__.return_value = "myhost_str"  # type: ignore
    myinst = MagicMock(spec=MInst, name="myinst")
    myinst.__str__.return_value = "myinst_str"  # type: ignore
    myhost.list_hosts_instances.return_value = [myinst]

    my_rhost = MagicMock(spec=RemoteHosts, name="my_rhost")
    my_rhost.hosts = ["myhost.foo.bar"]
    m_sr.remote.return_value.query.return_value = my_rhost
    m_sr.mysql.return_value.get_dbs.return_value = myhost

    args = Namespace(dc=None, task_id=None, hostnames=None)
    run(args, m_sr)

    cleaned_log = "\n".join(r.getMessage().rstrip() for r in caplog.records)
    expected = """\
Provisional plan:
Hostname             Instance count
myhost_str           1
[cookbooks.sre.mysql.sanitarium_restart.stop_repl] Running STOP SLAVE on myinst_str
[cookbooks.sre.mysql.sanitarium_restart.stop_mariadb] Stopping MariaDB on myinst_str
[cookbooks.sre.mysql.sanitarium_restart.start_mariadb] Starting MariaDB on myinst_str
[cookbooks.sre.mysql.sanitarium_restart.start_repl] Starting replication on myinst_str"""
    assert cleaned_log == expected
