"""Unit tests for the sre.wdqs.data-transfer transferpy integration."""

import importlib

from unittest import mock

import pytest


data_transfer = importlib.import_module("cookbooks.sre.wdqs.data-transfer")


def make_runner():
    """Build a runner without the spicerack setup that __init__ performs."""
    runner = object.__new__(data_transfer.DataTransferRunner)
    runner.r_source = "wdqs2008.codfw.wmnet"
    runner.r_dest = "wdqs1016.eqiad.wmnet"
    runner.encrypt = False
    return runner


def test_raises_when_transferpy_reports_failure():
    """A failed transfer must abort before data_loaded, Kafka offsets, and repooling."""
    runner = make_runner()

    with mock.patch.object(data_transfer, "Transferer") as transferer:
        transferer.return_value.run.return_value = [-1]

        with pytest.raises(RuntimeError, match="wikidata.jnl"):
            runner.transfer_datafiles("/srv/wdqs", ["/srv/wdqs/wikidata.jnl"])


def test_transfers_every_file_when_all_succeed():
    """A clean run must transfer each file and raise nothing."""
    runner = make_runner()
    files = ["/srv/wdqs/wikidata.jnl", "/srv/wdqs/data_loaded"]

    with mock.patch.object(data_transfer, "Transferer") as transferer:
        transferer.return_value.run.return_value = [0]

        runner.transfer_datafiles("/srv/wdqs", files)

    assert transferer.return_value.run.call_count == len(files)


def test_stops_at_the_first_failed_file():
    """Remaining files must not be transferred once one has failed."""
    runner = make_runner()

    with mock.patch.object(data_transfer, "Transferer") as transferer:
        transferer.return_value.run.side_effect = [[0], [2], [0]]

        with pytest.raises(RuntimeError, match="aliases.map"):
            runner.transfer_datafiles(
                "/srv/wdqs",
                ["/srv/wdqs/categories.jnl", "/srv/wdqs/aliases.map", "/srv/wdqs/wikidata.jnl"],
            )

    assert transferer.return_value.run.call_count == 2
