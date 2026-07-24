"""
Unit tests for sre.mysql.multiinstance_reboot

Test using:
tox -e py313-lint_unit -- tests/unit/sre/mysql/multiinstance_reboot_test.py -vv
"""

import logging
from argparse import Namespace
from datetime import datetime, timezone
from unittest import mock
from unittest.mock import MagicMock

from cookbooks.sre.mysql.multiinstance_reboot import MultiinstanceRebootRunner
from pytest import fixture
from spicerack.apt import AptGetHosts
from spicerack.mysql import Instance

log = logging.getLogger(__name__)


def mock_run_sync(*args, **kwargs):
    cmd = args[0] if args else ""
    log.info(f"Mock-running '{cmd}'")
    return iter([(MagicMock(), MagicMock())])


def mock_filter_objects(tags, name):
    obj = MagicMock()
    obj.__repr__ = lambda self: f'name="{name}"'
    return [obj]


def mock_update_objects(changes, objects):
    log.info(f"Mock-running 'confctl update_objects {changes},{objects}'")


def mock_icinga_hosts(hosts):
    m = MagicMock()
    m.wait_for_optimal.side_effect = lambda: log.info(f"Mock-running 'icinga_hosts.wait_for_optimal {hosts}'")
    return m


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.INFO)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


@fixture(autouse=True)
def m_sr():
    with mock.patch("spicerack.Spicerack", autospec=True) as m:
        yield m


@fixture(autouse=True)
def m_cursor():
    with mock.patch("spicerack.mysql.Instance.cursor") as m:
        m.return_value.__enter__.return_value = (mock.MagicMock(), mock.MagicMock())
        yield


@fixture(autouse=True)
def m_step():
    with mock.patch("cookbooks.sre.mysql.multiinstance_reboot.step") as m:
        yield m


@fixture(autouse=True)
def m_datetime():
    with mock.patch("cookbooks.sre.mysql.multiinstance_reboot.datetime") as m:
        m.now.return_value = datetime(2026, 5, 22, 0, 0, 0, tzinfo=timezone.utc)
        yield m


def test_run_clouddb_multiinstance(
    m_sr,
    caplog,
):
    mock_host = MagicMock()
    mock_host.hosts = ["clouddb1001.eqiad.wmnet"]
    mock_host.__str__ = MagicMock(return_value="clouddb1001.eqiad.wmnet")
    m_sr.remote.return_value.query.return_value = [mock_host]

    mock_host.run_sync.side_effect = mock_run_sync

    m_sr.apt_get.return_value = AptGetHosts(mock_host)
    # Ignore Spicerack's own apt logging, we already log it with mock_run_sync
    logging.getLogger("spicerack.apt").setLevel(logging.WARNING)

    mock_mysql_dbs = MagicMock()
    mock_instance_s1 = Instance(mock_host, name="s1")
    mock_instance_s2 = Instance(mock_host, name="s2")
    mock_mysql_dbs.list_hosts_instances.return_value = [mock_instance_s1, mock_instance_s2]
    m_sr.mysql.return_value.get_dbs.return_value = mock_mysql_dbs

    m_confctl = m_sr.confctl.return_value
    m_confctl.filter_objects.side_effect = mock_filter_objects
    m_confctl.update_objects.side_effect = mock_update_objects

    m_sr.icinga_hosts.side_effect = mock_icinga_hosts

    args = Namespace(query="clouddb*", repool=True, upgrade=True, task_id="T12345", reason="Rebooting")
    runner = MultiinstanceRebootRunner(args, m_sr)
    runner.run()

    exp = """\
INFO Mock-running 'confctl update_objects {'pooled': 'no'},[name="clouddb1001.eqiad.wmnet"]'
INFO Mock-running '/usr/local/bin/mysql --socket /run/mysqld/mysqld.s1.sock --batch --execute "STOP SLAVE"'
INFO Mock-running '/usr/bin/systemctl stop mariadb@s1.service'
INFO Mock-running '/usr/local/bin/mysql --socket /run/mysqld/mysqld.s2.sock --batch --execute "STOP SLAVE"'
INFO Mock-running '/usr/bin/systemctl stop mariadb@s2.service'
INFO Mock-running 'DEBIAN_FRONTEND=noninteractive /usr/bin/apt-get --quiet --yes --option Dpkg::Options::="--force-confdef" --option Dpkg::Options::="--force-confold" dist-upgrade'
INFO Mock-running 'mountpoint /srv'
INFO Mock-running 'umount /srv'
INFO Mock-running 'swapoff -a'
INFO Mock-running '/usr/bin/systemctl set-environment MYSQLD_OPTS="--skip-slave-start"'
INFO Mock-running '/usr/bin/systemctl start mariadb@s1.service'
INFO Mock-running '$(readlink -f /usr/local/bin/mysql_upgrade) --socket /run/mysqld/mysqld.s1.sock --force'
INFO Mock-running '/usr/bin/systemctl restart mariadb@s1.service'
INFO Mock-running '/usr/local/bin/mysql --socket /run/mysqld/mysqld.s1.sock --batch --execute "START SLAVE"'
INFO Mock-running 'systemctl restart prometheus-mysqld-exporter@s1.service'
INFO Mock-running '/usr/bin/systemctl start mariadb@s2.service'
INFO Mock-running '$(readlink -f /usr/local/bin/mysql_upgrade) --socket /run/mysqld/mysqld.s2.sock --force'
INFO Mock-running '/usr/bin/systemctl restart mariadb@s2.service'
INFO Mock-running '/usr/local/bin/mysql --socket /run/mysqld/mysqld.s2.sock --batch --execute "START SLAVE"'
INFO Mock-running 'systemctl restart prometheus-mysqld-exporter@s2.service'
INFO Mock-running 'icinga_hosts.wait_for_optimal ['clouddb1001.eqiad.wmnet']'
INFO Mock-running 'confctl update_objects {'pooled': 'yes'},[name="clouddb1001.eqiad.wmnet"]'
"""
    assert caplog.text == exp
