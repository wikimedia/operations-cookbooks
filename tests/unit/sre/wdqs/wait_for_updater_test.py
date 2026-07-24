"""Unit tests for sre.wdqs.wait_for_updater."""

from unittest import mock

import pytest
from wmflib import decorators

from cookbooks.sre.wdqs import (
    UpdaterLagTooHigh,
    UpdaterMetricUnavailable,
    get_updater_lag,
    wait_for_updater,
)


# Bypass the retry relevant to each unit assertion
get_updater_lag_check = get_updater_lag.__wrapped__
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

    assert get_updater_lag_check(prometheus, "eqiad", remote_host) == 20.5

    prometheus.query.assert_called_once_with(
        "scalar(time() - blazegraph_lastupdated{instance=~'wdqs1023:919[35]'})", "eqiad")


def test_raises_when_lag_is_high():
    """It should raise so @retry keeps waiting while the updater catches up."""
    prometheus, remote_host = make_mocks([1784659207.9, "5000"])

    with mock.patch("cookbooks.sre.wdqs.get_updater_lag", return_value=5000):
        with pytest.raises(UpdaterLagTooHigh, match="too high"):
            wait_for_updater_check(prometheus, "eqiad", remote_host)


def test_raises_on_nan_lag():
    """NaN (missing series, or exporter reporting Blazegraph down) must not pass as healthy."""
    prometheus, remote_host = make_mocks([1784659207.9, "NaN"])

    with pytest.raises(UpdaterMetricUnavailable, match="No valid lag data"):
        get_updater_lag_check(prometheus, "eqiad", remote_host)


def test_raises_on_infinite_lag():
    """Non-finite values other than NaN must not pass as healthy either."""
    prometheus, remote_host = make_mocks([1784659207.9, "-Inf"])

    with pytest.raises(UpdaterMetricUnavailable, match="No valid lag data"):
        get_updater_lag_check(prometheus, "eqiad", remote_host)


def test_raises_on_empty_result():
    """An empty Prometheus response must not pass as healthy."""
    prometheus, remote_host = make_mocks([])

    with pytest.raises(UpdaterMetricUnavailable, match="Empty response"):
        get_updater_lag_check(prometheus, "eqiad", remote_host)


def test_metric_retry_recovers_when_lag_becomes_available():
    """The short retry should recover after the exporter is scraped."""
    prometheus, remote_host = make_mocks(None)
    prometheus.query.side_effect = [
        [1784659207.9, "NaN"],
        [1784659267.9, "20.5"],
    ]

    with mock.patch.object(decorators.time, "sleep") as sleep:
        assert get_updater_lag(prometheus, "eqiad", remote_host) == 20.5

    sleep.assert_called_once_with(60.0)


def test_metric_retry_is_bounded():
    """Unavailable metrics should fail after 30 attempts, not retry for days."""
    prometheus, remote_host = make_mocks([1784659207.9, "NaN"])

    with mock.patch.object(decorators.time, "sleep"):
        with pytest.raises(UpdaterMetricUnavailable):
            get_updater_lag(prometheus, "eqiad", remote_host)

    assert prometheus.query.call_count == 30


def test_metric_unavailable_escapes_long_catchup_retry():
    """The long retry must catch only valid-but-high lag."""
    prometheus, remote_host = make_mocks(None)

    with mock.patch(
        "cookbooks.sre.wdqs.get_updater_lag",
        side_effect=UpdaterMetricUnavailable("missing"),
    ) as metric:
        with pytest.raises(UpdaterMetricUnavailable, match="missing"):
            wait_for_updater(prometheus, "eqiad", remote_host)

    metric.assert_called_once_with(prometheus, "eqiad", remote_host)


def test_unclassified_value_error_escapes_long_catchup_retry():
    """A malformed Prometheus value must not inherit the high-lag retry."""
    prometheus, remote_host = make_mocks(None)

    with mock.patch(
        "cookbooks.sre.wdqs.get_updater_lag",
        side_effect=ValueError("malformed"),
    ) as metric:
        with pytest.raises(ValueError, match="malformed"):
            wait_for_updater(prometheus, "eqiad", remote_host)

    metric.assert_called_once_with(prometheus, "eqiad", remote_host)
