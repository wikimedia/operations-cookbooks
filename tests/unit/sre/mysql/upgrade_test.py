"""
Unit tests for sre.mysql.upgrade

Test using:
tox -e py313-lint_unit -- tests/unit/sre/mysql/upgrade_test.py -vv
"""

from argparse import Namespace
from datetime import datetime
from pytest import (
    fixture,
    raises,
)
from unittest import mock
from unittest.mock import MagicMock, patch
import logging

from cookbooks.sre.mysql.upgrade import (
    MInst,
    UpgradeMySQLRunner,
    get_db_instance,
)

log = logging.getLogger()


# Fixtures


@fixture(autouse=True)
def mock_durable_shell():
    with patch("cookbooks.sre.mysql.upgrade.ensure_shell_is_durable", autospec=True):
        yield


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


# Tests


def test_get_db_instance():
    mock_mysql = MagicMock()
    mock_dbs = MagicMock()
    mock_inst = MagicMock(spec=MInst)

    mock_mysql.get_dbs.return_value = mock_dbs
    mock_dbs.list_hosts_instances.return_value = [mock_inst]

    assert get_db_instance(mock_mysql, "db1001.eqiad.wmnet") == mock_inst
    mock_mysql.get_dbs.assert_called_once_with("db1001.eqiad.wmnet")


@patch("spicerack.Spicerack", autospec=True)
def test_run_multiple_hosts_raises(m_sr):
    mock_hosts = MagicMock()
    mock_hosts.__len__.return_value = 2  # simulate >1 matched host

    m_sr.remote.return_value.query.return_value = mock_hosts

    args = Namespace(
        query="db11[76-77]", repool=True, task_id="T12345", reason="Upgrading"
    )

    with raises(ValueError, match="Multiple hosts have been matched"):
        UpgradeMySQLRunner(args, m_sr)


@patch("cookbooks.sre.mysql.upgrade.sleep")
@patch("cookbooks.sre.mysql.upgrade.datetime", autospec=True)
@patch("cookbooks.sre.mysql.upgrade.get_db_instance", autospec=True)
@patch("cookbooks.sre.mysql.upgrade.confirm_on_failure", autospec=True)
@patch("spicerack.Spicerack", autospec=True)
def test_run(
    m_sr,
    m_confirm,
    m_gdbi,
    m_datetime,
    m_sleep,
    caplog,
):
    m_datetime.now.return_value = datetime(2026, 5, 22, 0, 0, 0)

    mock_host = MagicMock()
    mock_host.hosts = ["db1176.eqiad.wmnet"]
    # mock_host.__str__.return_value = "db1176.eqiad.wmnet"
    setattr(mock_host, "__str__", MagicMock(return_value="db1176.eqiad.wmnet"))

    m_sr.remote.return_value.query.return_value = mock_host
    m_sr.run_cookbook = mock.Mock()

    mock_dbi = MagicMock(spec=MInst)
    m_gdbi.return_value = mock_dbi

    def mock_run(func, cmd):
        assert func == mock_host.run_sync
        log.info(f"Mock-running '{cmd}'")

    m_confirm.side_effect = mock_run

    def mock_runcb(*a, **kw):
        log.info(f"Mock-running '{a}' '{kw}'")

    m_sr.run_cookbook.side_effect = mock_runcb

    args = Namespace(query="db1176", repool=True, task_id="T12345", reason="Upgrading")

    runner = UpgradeMySQLRunner(args, m_sr)
    runner.run()

    mock_host.wait_reboot_since.assert_called_once_with(datetime(2026, 5, 22, 0, 0, 0))
    mock_host.wait_reboot_since.assert_called_once()
    mock_dbi.wait_for_replication.assert_called_once()
    m_sr.icinga_hosts.assert_called_once_with(["db1176.eqiad.wmnet"])
    m_sr.icinga_hosts.return_value.wait_for_optimal.assert_called_once()

    exp = """\
INFO [cookbooks.sre.mysql.upgrade.depool] Depooling db1176.eqiad.wmnet
INFO Mock-running '('sre.mysql.depool', ['--reason', 'Upgrading db1176.eqiad.wmnet', '--task-id', 'T12345', 'db1176'])' '{'confirm': True}'
INFO [cookbooks.sre.mysql.upgrade.stop_mariadb] Stopping mariadb on db1176.eqiad.wmnet
INFO Mock-running 'mysql -e "stop slave;"'
INFO Mock-running 'systemctl stop mariadb'
INFO Mock-running 'DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold' dist-upgrade'
INFO Mock-running 'umount /srv'
INFO Mock-running 'swapoff -a'
INFO [cookbooks.sre.mysql.upgrade.reboot] Rebooting host
INFO [cookbooks.sre.mysql.upgrade.mysql_upgrade] Start MariaDB and run mysql_upgrade
INFO Mock-running 'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"'
INFO Mock-running 'systemctl start mariadb'
INFO Mock-running 'mysql_upgrade'
INFO Mock-running 'systemctl restart mariadb'
INFO Mock-running 'mysql -e "start slave;"'
INFO [cookbooks.sre.mysql.upgrade.restart_prom_exp] Restarting Prometheus exporter
INFO Mock-running 'systemctl restart prometheus-mysqld-exporter.service'
INFO [cookbooks.sre.mysql.upgrade.catchup_repl_s] Catching up replication lag on db1176.eqiad.wmnet before removing icinga downtime
INFO [cookbooks.sre.mysql.upgrade.wait_icinga_s] Waiting for icinga to go green for db1176.eqiad.wmnet
INFO Mock-running '('sre.mysql.pool', ['--reason', 'Upgrade of db1176.eqiad.wmnet completed', '--task-id', 'T12345', 'db1176'])' '{'confirm': True}'
"""
    assert caplog.text == exp
