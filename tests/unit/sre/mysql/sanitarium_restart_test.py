import logging
from argparse import Namespace

from cookbooks.sre.mysql.sanitarium_restart import run
from pytest import fixture
from spicerack.mysql import Instance as MInst
from spicerack.mysql import MysqlRemoteHosts
from spicerack.remote import RemoteHosts


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


def test_run(mocker, caplog) -> None:
    caplog.set_level(logging.INFO)

    mocker.patch("cookbooks.sre.mysql.sanitarium_restart.sleep")
    mocker.patch("cookbooks.sre.mysql.sanitarium_restart.ensure_shell_is_durable")
    mocker.patch("cookbooks.sre.mysql.sanitarium_restart.ask_confirmation")

    sr = mocker.MagicMock(name="Spicerack")

    myhost = mocker.MagicMock(spec=MysqlRemoteHosts, name="myhost")
    myhost.__str__.return_value = "myhost_str"
    myinst = mocker.MagicMock(spec=MInst, name="myinst")
    myinst.__str__.return_value = "myinst_str"
    myhost.list_hosts_instances.return_value = [myinst]

    my_rhost = mocker.MagicMock(spec=RemoteHosts, name="my_rhost")
    my_rhost.hosts = ["myhost.foo.bar"]

    sr.remote.return_value.query.return_value = my_rhost
    sr.mysql.return_value.get_dbs.return_value = myhost

    args = Namespace(dc=None, task_id=None, hostnames=None)
    run(args, sr)

    exp = """\
INFO Provisional plan:
INFO Hostname             Instance count
INFO myhost_str           1         
INFO [cookbooks.sre.mysql.sanitarium_restart.stop_repl] Running STOP SLAVE on myinst_str
INFO [cookbooks.sre.mysql.sanitarium_restart.stop_mariadb] Stopping MariaDB on myinst_str
INFO [cookbooks.sre.mysql.sanitarium_restart.start_mariadb] Starting MariaDB on myinst_str
INFO [cookbooks.sre.mysql.sanitarium_restart.start_repl] Starting replication on myinst_str
"""
    assert caplog.text == exp
