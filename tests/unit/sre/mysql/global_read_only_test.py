"""
Unit tests for sre.mysql.global-read-only
"""

from argparse import Namespace
from pytest import fixture

from unittest.mock import patch, MagicMock, DEFAULT as mock_default
import importlib
import logging

from conftool.extensions.dbconfig.action import ActionResult

gro = importlib.import_module("cookbooks.sre.mysql.global-read-only")

log = logging.getLogger()


# # Fixtures


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


def mock_sr():
    with patch.object(gro, "Spicerack", autospec=True) as mock_sr_class:
        mock_sr = mock_sr_class.return_value
        dbctl = mock_sr.dbctl()
        mysql = mock_sr.mysql()
        mysql.get_core_dbs.return_value.mrhs.list_hosts_instances.return_value = "meow"
        dbctl.config.commit().announce_message = "<<mock dbctl config commit announce msg>>"

        mock_sr.mediawiki.return_value.get_master_datacenter.return_value = "eqiad"

        mock_inst = MagicMock()
        mysql.get_core_dbs.return_value.list_hosts_instances.return_value = [mock_inst]

        mock_sr.sal_logger.info.side_effect = lambda msg: log.info(f"Mock SAL log <<{msg}>>")

        setattr(gro, "ask_confirmation", lambda msg: log.info(f"Mock ask '{msg}'"))
        gro.time.sleep = MagicMock()

        # has diffs
        diff_ret = ActionResult(messages=[], success=True, exit_code=1)
        dbctl.config.diff.return_value = (diff_ret, ["bogus_line"])

        dbctl.section.set_readonly.return_value = ActionResult(success=True, exit_code=0)

        # mock_sr.dbctl.return_value.config.diff.return_value = (diff_ret, None)

        def z(task, msg, raises=False):
            log.info(f"mock phabricator task_comment '{task}' '{msg}'")

        mock_sr.phabricator.return_value.task_comment.side_effect = z

        return mock_sr


# # Tests


def test_format_msg() -> None:
    msg = gro._format_msg(Namespace(task_id=None, reason=None), ["s1", "s2"], True)
    assert msg == "Setting sections s1, s2 as read-only"

    msg = gro._format_msg(Namespace(task_id="T0", reason="a b c"), ["s1", "s2"], True)
    assert msg == "Setting sections s1, s2 as read-only for T0: 'a b c'"


def test_global_read_only(caplog) -> None:
    sr = mock_sr()
    args = Namespace(
        action="set-ro", sections="test-s4,test-s1", ignore_dirty_dbctl=True, reason="my test", task_id="T0"
    )
    gro.run(args, sr)

    exp = """\
INFO Primary DC: eqiad
INFO Mock ask 'CAUTION: Setting sections test-s1, test-s4 as read-only for T0: 'my test' - are you really sure?'
INFO Going read-only: first dbctl then MariaDB
DEBUG Setting dbctl test-s1 in codfw
DEBUG Setting dbctl test-s1 in eqiad
DEBUG Setting dbctl test-s4 in codfw
DEBUG Setting dbctl test-s4 in eqiad
INFO Changes:
DEBUG bogus_line
INFO Committing dbctl config: Setting sections test-s1, test-s4 as read-only for T0: 'my test'
INFO Mock SAL log <<Dbctl change: Setting sections test-s1, test-s4 as read-only for T0: 'my test'>>
INFO <<mock dbctl config commit announce msg>>
INFO Mock SAL log <<MariaDB change: Setting sections test-s1, test-s4 as read-only for T0: 'my test'>>
INFO Setting MariaDB test-s1 in eqiad read-only
INFO Setting MariaDB test-s4 in eqiad read-only
INFO Updating Phabricator
INFO mock phabricator task_comment 'T0' 'Setting sections test-s1, test-s4 as read-only for T0: 'my test''
"""
    assert caplog.text == exp


def test_global_read_only_fail_on_a_master(caplog) -> None:
    sr = mock_sr()
    args = Namespace(
        action="set-ro", sections="test-s4,test-s1", ignore_dirty_dbctl=True, reason="my test", task_id="T0"
    )

    # Mock mysql() -> get_core_dbs() -> ... run_query() to fail once
    inst = sr.mysql.return_value.get_core_dbs.return_value.list_hosts_instances.return_value[0]
    inst.run_query.side_effect = [Exception("run_query mock error"), mock_default]

    gro.run(args, sr)

    exp = """\
INFO Primary DC: eqiad
INFO Mock ask 'CAUTION: Setting sections test-s1, test-s4 as read-only for T0: 'my test' - are you really sure?'
INFO Going read-only: first dbctl then MariaDB
DEBUG Setting dbctl test-s1 in codfw
DEBUG Setting dbctl test-s1 in eqiad
DEBUG Setting dbctl test-s4 in codfw
DEBUG Setting dbctl test-s4 in eqiad
INFO Changes:
DEBUG bogus_line
INFO Committing dbctl config: Setting sections test-s1, test-s4 as read-only for T0: 'my test'
INFO Mock SAL log <<Dbctl change: Setting sections test-s1, test-s4 as read-only for T0: 'my test'>>
INFO <<mock dbctl config commit announce msg>>
INFO Mock SAL log <<MariaDB change: Setting sections test-s1, test-s4 as read-only for T0: 'my test'>>
INFO Setting MariaDB test-s1 in eqiad read-only
ERROR Error test-s1 run_query mock error
INFO Setting MariaDB test-s4 in eqiad read-only
INFO Updating Phabricator
INFO mock phabricator task_comment 'T0' 'Setting sections test-s1, test-s4 as read-only for T0: 'my test'
Not all MariaDB masters were updated successfully:
TODO: ['test-s1', 'test-s4']
DONE: ['test-s4']'
"""
    assert caplog.text == exp


def test_global_read_write(caplog) -> None:
    sr = mock_sr()
    args = Namespace(
        action="set-rw", sections="test-s4,test-s1", ignore_dirty_dbctl=True, reason="my test", task_id="T0"
    )
    gro.run(args, sr)

    exp = """\
INFO Primary DC: eqiad
INFO Mock ask 'CAUTION: Setting sections test-s1, test-s4 as read-write for T0: 'my test' - are you really sure?'
INFO Going read-write: first MariaDB then dbctl
INFO Mock SAL log <<MariaDB change: Setting sections test-s1, test-s4 as read-write for T0: 'my test'>>
INFO Setting MariaDB test-s1 in eqiad read-write
INFO Setting MariaDB test-s4 in eqiad read-write
DEBUG Setting dbctl test-s1 in codfw
DEBUG Setting dbctl test-s1 in eqiad
DEBUG Setting dbctl test-s4 in codfw
DEBUG Setting dbctl test-s4 in eqiad
INFO Changes:
DEBUG bogus_line
INFO Committing dbctl config: Setting sections test-s1, test-s4 as read-write for T0: 'my test'
INFO Mock SAL log <<Dbctl change: Setting sections test-s1, test-s4 as read-write for T0: 'my test'>>
INFO <<mock dbctl config commit announce msg>>
INFO Updating Phabricator
INFO mock phabricator task_comment 'T0' 'Setting sections test-s1, test-s4 as read-write for T0: 'my test''
"""
    assert caplog.text == exp


def test_global_read_write_fail_on_a_master(caplog) -> None:
    sr = mock_sr()
    args = Namespace(
        action="set-rw", sections="test-s4,test-s1", ignore_dirty_dbctl=True, reason="my test", task_id="T0"
    )

    # Mock mysql() -> get_core_dbs() -> ... run_query() to fail once
    inst = sr.mysql.return_value.get_core_dbs.return_value.list_hosts_instances.return_value[0]
    inst.run_query.side_effect = [Exception("run_query mock error"), mock_default]

    gro.run(args, sr)

    exp = """\
INFO Primary DC: eqiad
INFO Mock ask 'CAUTION: Setting sections test-s1, test-s4 as read-write for T0: 'my test' - are you really sure?'
INFO Going read-write: first MariaDB then dbctl
INFO Mock SAL log <<MariaDB change: Setting sections test-s1, test-s4 as read-write for T0: 'my test'>>
INFO Setting MariaDB test-s1 in eqiad read-write
ERROR Error test-s1 run_query mock error
INFO Setting MariaDB test-s4 in eqiad read-write
ERROR Not all MariaDB masters were switched to read-write successfully!
ERROR TODO: ['test-s1', 'test-s4']
ERROR DONE: ['test-s4']
INFO dbctl will be set to read-write only for the MariaDB masters that have been switched
DEBUG Setting dbctl test-s4 in codfw
DEBUG Setting dbctl test-s4 in eqiad
INFO Changes:
DEBUG bogus_line
INFO Committing dbctl config: Setting sections test-s1, test-s4 as read-write for T0: 'my test'
INFO Mock SAL log <<Dbctl change: Setting sections test-s1, test-s4 as read-write for T0: 'my test'>>
INFO <<mock dbctl config commit announce msg>>
INFO Updating Phabricator
INFO mock phabricator task_comment 'T0' 'Setting sections test-s1, test-s4 as read-write for T0: 'my test'
Not all MariaDB masters were updated successfully:
TODO: ['test-s1', 'test-s4']
DONE: ['test-s4']'
"""
    assert caplog.text == exp
