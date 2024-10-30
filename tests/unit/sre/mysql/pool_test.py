"""
Unit tests for sre.mysql.pool
Test using:
tox -e py311-unit -- tests/unit/sre/mysql/pool_test.py -vv
"""

# pylint: disable=missing-docstring,line-too-long
# flake8: noqa: D103
from unittest import mock

from cookbooks.sre.mysql.pool import (
    _count_tcp_connections_port_3306,
    _fetch_instance_connections_count_wikiusers,
    _fetch_instance_connections_count_detailed,
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

    r = _fetch_instance_connections_count_detailed(mock_i)

    cur.execute.assert_called_once()
    cur.fetchall.assert_called_once()
    mock_i.check_warnings.assert_called_once_with(cur)
