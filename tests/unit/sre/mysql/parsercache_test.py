"""
Unit tests for sre.mysql.parsercache

Test using:
tox -e py313-lint_unit -- tests/unit/sre/mysql/parsercache_test.py -vv
"""

import logging
from argparse import Namespace

from cookbooks.sre.mysql.parsercache import depool, pool
from pytest import fixture

log = logging.getLogger()
log.setLevel(logging.DEBUG)


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


def test_pool(mocker, caplog):
    sr = mocker.MagicMock(name="Spicerack")
    al = mocker.MagicMock(name="IcingaHosts")
    dbctl = mocker.MagicMock(name="Dbctl")

    ret = mocker.MagicMock(success=True, exit_code=0)
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


def test_depool(mocker, caplog):
    sr = mocker.MagicMock(name="Spicerack")
    al = mocker.MagicMock(name="IcingaHosts")
    dbctl = mocker.MagicMock(name="Dbctl")

    ret = mocker.MagicMock(success=True, exit_code=0)
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
