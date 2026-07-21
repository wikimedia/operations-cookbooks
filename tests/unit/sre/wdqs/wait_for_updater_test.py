"""Unit tests for sre.wdqs.wait_for_updater."""

from unittest import mock

import pytest

from cookbooks.sre.wdqs import wait_for_updater


# Bypass the @retry decorator (1000 tries with 10 minute delays)
# and test the underlying check directly
wait_for_updater_check = wait_for_updater.__wrapped__


def make_mocks(result):
    """Return (prometheus, remote_host) mocks with a canned query result."""
    prometheus = mock.MagicMock()
    prometheus.query.return_value = result
    remote_host = mock.MagicMock()
    remote_host.hosts = ["wdqs1023.eqiad.wmnet"]
    return prometheus, remote_host


def test_passes_when_lag_is_low():
    """It should return without raising when the reported lag is under the threshold."""
    prometheus, remote_host = make_mocks([1784659207.9, "20.5"])

    wait_for_updater_check(prometheus, "eqiad", remote_host)

    prometheus.query.assert_called_once_with(
        "scalar(time() - blazegraph_lastupdated{instance=~'wdqs1023:919[35]'})", "eqiad")


def test_raises_when_lag_is_high():
    """It should raise so @retry keeps waiting while the updater catches up."""
    prometheus, remote_host = make_mocks([1784659207.9, "5000"])

    with pytest.raises(ValueError, match="too high"):
        wait_for_updater_check(prometheus, "eqiad", remote_host)


def test_raises_on_nan_lag():
    """NaN (missing series, or exporter reporting Blazegraph down) must not pass as healthy."""
    prometheus, remote_host = make_mocks([1784659207.9, "NaN"])

    with pytest.raises(ValueError, match="No valid lag data"):
        wait_for_updater_check(prometheus, "eqiad", remote_host)


def test_raises_on_infinite_lag():
    """Non-finite values other than NaN must not pass as healthy either."""
    prometheus, remote_host = make_mocks([1784659207.9, "-Inf"])

    with pytest.raises(ValueError, match="No valid lag data"):
        wait_for_updater_check(prometheus, "eqiad", remote_host)


def test_raises_on_empty_result():
    """An empty Prometheus response must not pass as healthy."""
    prometheus, remote_host = make_mocks([])

    with pytest.raises(ValueError, match="Empty response"):
        wait_for_updater_check(prometheus, "eqiad", remote_host)
