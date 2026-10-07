"""Tests for the check-user-leftovers cookbook."""

import importlib
from argparse import ArgumentTypeError
from unittest import mock

import pytest


check_user_leftovers = importlib.import_module("cookbooks.sre.hadoop.check-user-leftovers")


def _make_runner():
    spicerack = mock.MagicMock()
    args = mock.MagicMock(username="jdoe", output=None, max_entries=200)
    return check_user_leftovers.CheckUserLeftoversRunner(args, spicerack)


def test_validate_username_accepts_normal_username():
    """A plain username should be returned unchanged."""
    assert check_user_leftovers.validate_username("jdoe") == "jdoe"


@pytest.mark.parametrize("value", ["jdoe; rm -rf /", "jdoe$(whoami)", "jdoe doe", "", "jdoe`id`"])
def test_validate_username_rejects_shell_metacharacters(value):
    """Anything that isn't a plain username must be rejected before it reaches a remote shell command."""
    with pytest.raises(ArgumentTypeError):
        check_user_leftovers.validate_username(value)


def test_parse_find_output_skips_empty_output_guard():
    """The unconditional end-of-output guard line must not become an entry."""
    assert check_user_leftovers.parse_find_output(check_user_leftovers.EMPTY_OUTPUT_GUARD) == []


def test_parse_find_output_parses_entries():
    """Each find(1) -printf line should become one structured entry."""
    raw = "d|4096|1700000000.0|1700000000.0|/home/jdoe\nf|123|1700000100.5|1700000100.5|/home/jdoe/notes.txt"

    entries = check_user_leftovers.parse_find_output(raw)

    assert entries == [
        {
            "path": "/home/jdoe",
            "type": "d",
            "size": 4096,
            "mtime": "2023-11-14T22:13:20+00:00",
            "ctime": "2023-11-14T22:13:20+00:00",
        },
        {
            "path": "/home/jdoe/notes.txt",
            "type": "f",
            "size": 123,
            "mtime": "2023-11-14T22:15:00.500000+00:00",
            "ctime": "2023-11-14T22:15:00.500000+00:00",
        },
    ]


def test_parse_find_output_skips_unparseable_lines():
    """A line that doesn't have the expected number of fields should be skipped, not raise."""
    assert check_user_leftovers.parse_find_output("this is not|the expected format") == []


def test_parse_hdfs_ls_output_parses_files_and_directories():
    """Standard `hdfs dfs -ls -R` output should be parsed into path/type/size/owner/group/mtime."""
    raw = (
        "Found 2 items\n"
        "drwxr-xr-x   - jdoe supergroup          0 2023-11-14 22:13 /user/jdoe\n"
        "-rw-r--r--   3 jdoe supergroup       1234 2023-11-14 22:15 /user/jdoe/data.csv"
    )

    entries = check_user_leftovers.parse_hdfs_ls_output(raw)

    assert entries == [
        {
            "path": "/user/jdoe",
            "type": "d",
            "size": 0,
            "owner": "jdoe",
            "group": "supergroup",
            "mtime": "2023-11-14T22:13",
        },
        {
            "path": "/user/jdoe/data.csv",
            "type": "f",
            "size": 1234,
            "owner": "jdoe",
            "group": "supergroup",
            "mtime": "2023-11-14T22:15",
        },
    ]


def test_parse_hdfs_ls_output_skips_empty_output_guard():
    """The unconditional end-of-output guard line must not be mistaken for a listing line."""
    assert check_user_leftovers.parse_hdfs_ls_output(check_user_leftovers.EMPTY_OUTPUT_GUARD) == []


def test_summarize_entries_keeps_full_listing_under_the_threshold():
    """Below max_entries, the raw entry list should be returned as-is."""
    entries = [{"path": "/home/jdoe/a", "size": 1}, {"path": "/home/jdoe/b", "size": 2}]

    result = check_user_leftovers.summarize_entries(entries, "/home/jdoe", max_entries=10)

    assert result == {"summarized": False, "entries": entries}


def test_summarize_entries_collapses_by_top_level_subdirectory_above_threshold():
    """Above max_entries, entries should be collapsed into per-top-level-subdirectory counts and sizes."""
    entries = [
        {"path": "/home/jdoe/dirA/file1", "size": 10},
        {"path": "/home/jdoe/dirA/file2", "size": 20},
        {"path": "/home/jdoe/dirB/file3", "size": 5},
        {"path": "/home/jdoe", "size": 4096},
    ]

    result = check_user_leftovers.summarize_entries(entries, "/home/jdoe", max_entries=2)

    assert result == {
        "summarized": True,
        "total_entry_count": 4,
        "entries": [
            {"directory": ".", "count": 1, "total_size": 4096},
            {"directory": "dirA", "count": 2, "total_size": 30},
            {"directory": "dirB", "count": 1, "total_size": 5},
        ],
    }


def test_run_returns_zero_even_when_leftovers_are_found(capsys):
    """Finding leftover data is a normal outcome, not a failure: the exit code must stay 0."""
    runner = _make_runner()
    found_check = {
        "name": "Statbox homedir",
        "hosts_query": "stat1*",
        "hosts_checked": 1,
        "unreachable_hosts": [],
        "results": {"stat1004.eqiad.wmnet": {"found": True, "summarized": False, "entries": []}},
    }

    with mock.patch.object(runner, "_run_check", return_value=found_check):
        exit_code = runner.run()

    assert exit_code == 0
    assert '"found": true' in capsys.readouterr().out.lower()


def test_run_returns_one_when_a_check_could_not_run():
    """A check that never ran at all (e.g. an unresolvable hosts query) is a real error, unlike leftover data."""
    runner = _make_runner()
    error_check = {"name": "Statbox homedir", "hosts_query": "stat1*", "error": "No hosts found for query"}

    with mock.patch.object(runner, "_run_check", return_value=error_check):
        exit_code = runner.run()

    assert exit_code == 1


def test_run_check_recovers_hosts_that_succeeded_despite_a_remote_execution_error():
    """One host failing (e.g. a genuinely unreachable host) must not wipe out the other hosts' results.

    This is a regression test for a real issue I encountered: run_sync() raises RemoteExecutionError if a
    single host returns a non-zero exit code, even if every other host succeeds. So we need to recover
    the successful hosts' output instead of discarding it.
    """
    runner = _make_runner()
    succeeded_hosts = ["stat1008.eqiad.wmnet", "stat1009.eqiad.wmnet"]
    output = mock.MagicMock()
    output.message.return_value = check_user_leftovers.EMPTY_OUTPUT_GUARD.encode()
    partial_results = iter([(succeeded_hosts, output)])
    error = check_user_leftovers.RemoteExecutionError(1, "Cumin execution failed", partial_results)

    hosts = mock.MagicMock()
    hosts.hosts = succeeded_hosts + ["stat1010.eqiad.wmnet"]
    hosts.run_sync.side_effect = error
    runner.remote.query.return_value = hosts

    check = runner._run_check(
        "Statbox userarchive", "stat1*", "find /var/userarchive ...", check_user_leftovers.parse_find_output
    )

    assert check["results"] == {
        "stat1008.eqiad.wmnet": {"found": False},
        "stat1009.eqiad.wmnet": {"found": False},
    }
    assert check["unreachable_hosts"] == ["stat1010.eqiad.wmnet"]
