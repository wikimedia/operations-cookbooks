"""Tests for the Hadoop rolling restart cookbook."""

import importlib
from datetime import datetime, timezone
from unittest import mock

import pytest
from ClusterShell.NodeSet import NodeSet


roll_restart_workers = importlib.import_module("cookbooks.sre.hadoop.roll-restart-workers")


@mock.patch.object(roll_restart_workers, "ensure_shell_is_durable")
def test_skip_hosts_filters_workers_before_selecting_journal_nodes(mocked_ensure_shell_is_durable):
    """The skip query should filter workers and then select their journal nodes."""
    spicerack = mock.MagicMock()
    remote = spicerack.remote.return_value
    workers = mock.MagicMock()
    workers.hosts = {"worker1", "worker2"}
    journal_workers = mock.MagicMock()
    journal_workers.hosts = {"worker2", "worker3"}
    remote.query.side_effect = (workers, journal_workers)
    args = roll_restart_workers.RollRestartWorkers(spicerack).argument_parser().parse_args(
        ["--skip-hosts", "P{an-worker100[1-3]*}", "analytics"]
    )

    roll_restart_workers.RollRestartWorkersRunner(args, spicerack)

    remote.query.assert_has_calls(
        [
            mock.call("A:hadoop-worker and not (P{an-worker100[1-3]*})"),
            mock.call("A:hadoop-hdfs-journal"),
        ]
    )
    journal_workers.get_subset.assert_called_once_with({"worker2"})
    mocked_ensure_shell_is_durable.assert_called_once_with()


@pytest.mark.parametrize('timestamp', ['2026-10-01T00:00:00', '2026-10-01T02:00:00+02:00'])
def test_start_datetime_parser(timestamp):
    """Naive and timezone-aware cutoffs are normalized to UTC."""
    args = roll_restart_workers.RollRestartWorkers(mock.MagicMock()).argument_parser().parse_args(
        ['analytics', '--start-datetime', timestamp])
    assert args.start_datetime == datetime(2026, 10, 1, tzinfo=timezone.utc)


def test_invalid_start_datetime():
    """Reject malformed cutoffs before running any operations."""
    parser = roll_restart_workers.RollRestartWorkers(mock.MagicMock()).argument_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(['analytics', '--start-datetime', 'invalid'])


@mock.patch.object(roll_restart_workers, 'ensure_shell_is_durable')
@pytest.mark.parametrize('timestamp,should_restart', [
    ('Wed 2026-09-30 23:59:59 UTC', True),
    ('Thu 2026-10-01 00:00:00 UTC', False),
    ('Thu 2026-10-01 00:00:01 UTC', False),
    ('', True),
])
def test_restart_cutoff(mocked_ensure_shell_is_durable, timestamp, should_restart):
    """Filter each daemon independently, including the exact cutoff and missing timestamps."""
    spicerack = mock.MagicMock()
    args = roll_restart_workers.RollRestartWorkers(spicerack).argument_parser().parse_args(
        ['analytics', '--start-datetime', '2026-10-01T00:00:00'])
    runner = roll_restart_workers.RollRestartWorkersRunner(args, spicerack)
    workers = mock.MagicMock()
    output = mock.Mock()
    output.lines.return_value = [timestamp.encode()]
    workers.run_sync.return_value = [(NodeSet('worker[1-2]'), output)]

    runner._restart_service(workers, 'hadoop-hdfs-datanode', batch_size=2, batch_sleep=120)

    workers.run_sync.assert_called_once_with(
        'TZ=UTC systemctl show hadoop-hdfs-datanode --property=ExecMainStartTimestamp --value', is_safe=True)
    if should_restart:
        workers.get_subset.assert_called_once_with(NodeSet('worker[1-2]'))
        workers.get_subset.return_value.run_sync.assert_called_once_with(
            'systemctl restart hadoop-hdfs-datanode', batch_size=2, batch_sleep=120)
    else:
        workers.get_subset.assert_not_called()


@mock.patch.object(roll_restart_workers, 'ensure_shell_is_durable')
def test_without_cutoff_restarts_all_services(mocked_ensure_shell_is_durable):
    """Omitting the cutoff preserves the existing restart commands and batches."""
    spicerack = mock.MagicMock()
    args = roll_restart_workers.RollRestartWorkers(spicerack).argument_parser().parse_args(['analytics'])
    runner = roll_restart_workers.RollRestartWorkersRunner(args, spicerack)
    runner.run()
    runner.hadoop_workers.run_sync.assert_has_calls([
        mock.call('systemctl restart hadoop-yarn-nodemanager', batch_size=5, batch_sleep=30.0),
        mock.call('systemctl restart hadoop-hdfs-datanode', batch_size=2, batch_sleep=120.0),
    ])
    runner.hadoop_hdfs_journal_workers.run_sync.assert_called_once_with(
        'systemctl restart hadoop-hdfs-journalnode', batch_size=1, batch_sleep=120.0)


@mock.patch.object(roll_restart_workers, "ensure_shell_is_durable")
def test_skip_hosts_can_exclude_all_journal_nodes(mocked_ensure_shell_is_durable):
    """An empty journal-node intersection should not create a RemoteHosts instance."""
    spicerack = mock.MagicMock()
    remote = spicerack.remote.return_value
    workers = mock.MagicMock()
    workers.hosts = {"worker1"}
    journal_workers = mock.MagicMock()
    journal_workers.hosts = {"worker2"}
    remote.query.side_effect = (workers, journal_workers)
    args = roll_restart_workers.RollRestartWorkers(spicerack).argument_parser().parse_args(
        ["--skip-hosts", "P{worker2}", "analytics"]
    )

    runner = roll_restart_workers.RollRestartWorkersRunner(args, spicerack)

    assert runner.hadoop_hdfs_journal_workers is None
    journal_workers.get_subset.assert_not_called()
    mocked_ensure_shell_is_durable.assert_called_once_with()
