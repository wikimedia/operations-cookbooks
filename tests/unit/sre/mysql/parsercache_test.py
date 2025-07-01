"""
Unit tests for sre.mysql.clone

Test using:
tox -e py311-unit -- tests/unit/sre/mysql/parsercache_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

import logging
from argparse import Namespace
from unittest import mock
from unittest.mock import MagicMock

import cookbooks.sre.mysql.parsercache
from cookbooks.sre.mysql.parsercache import pool, depool

logging.getLogger().setLevel(logging.DEBUG)


@mock.patch("spicerack.icinga.IcingaHosts", autospec=True)
@mock.patch("spicerack.dbctl.Dbctl", autospec=True)
@mock.patch("spicerack.Spicerack", autospec=True)
def test_pool(sr, dbctl, al, caplog):
    ret = MagicMock(success=True, exit_code=0)
    dbctl.config.diff.return_value = (ret, None)
    args = Namespace(reason="foo", task_id=None)
    pool(sr, args, al, dbctl, ["pc2000.codfw.wmnet"])

    logs = caplog.text
    assert "Waiting for dbctl diff to be empty" in logs
    assert "waiting for Icinga to be green" in logs
    assert "Removing Icinga downtime" in logs
    assert "Setting weight for pc2000 to 1" in logs
    assert "No changes to dbctl were made. Perhaps the hosts were already pooled in?" in logs


@mock.patch("spicerack.icinga.IcingaHosts", autospec=True)
@mock.patch("spicerack.dbctl.Dbctl", autospec=True)
@mock.patch("spicerack.Spicerack", autospec=True)
def test_depool(sr, dbctl, al, caplog):
    ret = MagicMock(success=True, exit_code=0)
    dbctl.config.diff.return_value = (ret, None)
    args = Namespace(reason="foo", downtime_hours=8, task_id=None)
    depool(sr, args, al, dbctl, ["pc2000.codfw.wmnet"])

    logs = caplog.text
    assert "Waiting for dbctl diff to be empty" in logs
    assert "Setting Icinga downtime" in logs
    assert "Setting weight for pc2000 to 0" in logs
    assert "No changes to dbctl were made. Perhaps the hosts were already depooled?" in logs
