"""
Unit tests for sre.mysql.pool
Test using:
tox -e py311-unit -- tests/unit/sre/mysql/pool_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103

import json
from pathlib import Path
from unittest import mock
import cookbooks.sre.mysql.pool
from cookbooks.sre.mysql.pool import (
    _fetch_instance_connections_count_wikiusers,
    _fetch_instance_connections_count_detailed,
    _check_depooling_last_instance,
)


@mock.patch("spicerack.mysql.Instance", autospec=True)
def test_fetch_instance_connections_count(mock_i):
    # what am I really testing?
    mock_i.fetch_one_row.return_value = {"cnt": 33}
    r = _fetch_instance_connections_count_wikiusers(mock_i)
    sql = "SELECT COUNT(*) AS cnt FROM information_schema.processlist WHERE user LIKE '%%wiki%%'"
    mock_i.fetch_one_row.assert_called_with(sql, ())
    assert r == 33


@mock.patch("spicerack.mysql.Instance", autospec=True)
def test_fetch_instance_connections_count_detailed(mock_i):
    cur = mock.MagicMock()
    mock_i.cursor.return_value.__enter__.return_value = (None, cur)
    cur.execute.return_value = None

    _ = _fetch_instance_connections_count_detailed(mock_i)

    cur.execute.assert_called_once()
    cur.fetchall.assert_called_once()
    mock_i.check_warnings.assert_called_once_with(cur)


def test_last_instance_depool():
    j = Path("tests/unit/sre/mysql/dbctl_config_get.json").read_text()
    conf = json.loads(j)

    ac = mock.MagicMock()
    cookbooks.sre.mysql.pool.ask_confirmation = ac
    _check_depooling_last_instance(conf, "pc2012", False)
    ac.assert_called()

    ac.reset_mock()
    _check_depooling_last_instance(conf, "pc1221", False)
    ac.assert_not_called()

    _check_depooling_last_instance(conf, "db2173", False)
    ac.assert_called()

    ac.reset_mock()
    _check_depooling_last_instance(conf, "db1248", False)  # vslow, 2 inst
    ac.assert_not_called()

    _check_depooling_last_instance(conf, "db2227", False)  # vslow, 1 inst
    ac.assert_called()
