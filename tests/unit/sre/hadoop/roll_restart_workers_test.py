"""Tests for the Hadoop rolling restart cookbook."""

import importlib
from unittest import mock


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
