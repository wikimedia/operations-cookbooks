"""
Unit tests for sre.mysql.clone

Test using:
tox -e py311-unit -- tests/unit/sre/mysql/parsercache_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

import logging
from argparse import Namespace
from pytest import fixture
from unittest import mock
from unittest.mock import MagicMock, patch

import cookbooks.sre.mysql.parsercache
from cookbooks.sre.mysql.parsercache import pool, depool

log = logging.getLogger()
log.setLevel(logging.DEBUG)


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))

    # mock_sr.admin_reason.return_value.owner = "<<mock owner>>"
    # mock_sr.admin_reason.return_value.reason = "<<mock reason>>"

    # def z(task, msg):
    #     log.info(f"mock phabricator task_comment '{task}' '{msg}'")

    # mock_sr.phabricator.return_value.task_comment.side_effect = z

    # yield mock_sr


@mock.patch("spicerack.icinga.IcingaHosts", autospec=True)
@mock.patch("spicerack.dbctl.Dbctl", autospec=True)
@mock.patch("spicerack.Spicerack", autospec=True)
def test_pool(sr, dbctl, al, caplog):
    ret = MagicMock(success=True, exit_code=0)
    dbctl.config.diff.return_value = (ret, None)
    dbctl.instance.weight().announce_message = "<<mock dbctl weight announce msg>>"
    args = Namespace(reason="foo", task_id=None, section="pc0")
    pool(sr, args, al, dbctl, ["pc2000.codfw.wmnet"])

    exp = """\
INFO Preparing to pool all hosts in 'pc0': pc2000.codfw.wmnet
INFO [cookbooks.sre.mysql.parsercache.pool] Rechecking and waiting for Icinga to be green
INFO [cookbooks.sre.mysql.parsercache.pool] Removing Icinga downtime if any
INFO [cookbooks.sre.mysql.parsercache.dbctl] Waiting for dbctl diff to be empty
INFO [cookbooks.sre.mysql.parsercache.pool] Setting weight for pc2000 to 1
INFO <<mock dbctl weight announce msg>>
INFO No changes to dbctl were made. Perhaps the hosts were already pooled in?
"""
    assert caplog.text == exp


@mock.patch("spicerack.icinga.IcingaHosts", autospec=True)
@mock.patch("spicerack.dbctl.Dbctl", autospec=True)
@mock.patch("spicerack.Spicerack", autospec=True)
def test_depool(sr, dbctl, al, caplog):
    ret = MagicMock(success=True, exit_code=0)
    dbctl.config.diff.return_value = (ret, None)

    dbctl.instance.pool().announce_message = "<<mock dbctl pool announce msg>>"
    dbctl.instance.depool().announce_message = "<<mock dbctl pool announce msg>>"
    dbctl.instance.weight().announce_message = "<<mock dbctl weight announce msg>>"
    dbctl.config.commit().announce_message = "<<mock dbctl config commit announce msg>>"

    args = Namespace(reason="foo", downtime_hours=8, task_id=None, section="pc0")
    depool(sr, args, al, dbctl, ["pc2000.codfw.wmnet"])

    exp = """\
INFO Preparing to depool all hosts in 'pc0': pc2000.codfw.wmnet
INFO [cookbooks.sre.mysql.parsercache.pool] Setting Icinga downtime
INFO [cookbooks.sre.mysql.parsercache.dbctl] Waiting for dbctl diff to be empty
INFO [cookbooks.sre.mysql.parsercache.pool] Setting weight for pc2000 to 0
INFO <<mock dbctl weight announce msg>>
INFO No changes to dbctl were made. Perhaps the hosts were already depooled?
"""
    assert caplog.text == exp
