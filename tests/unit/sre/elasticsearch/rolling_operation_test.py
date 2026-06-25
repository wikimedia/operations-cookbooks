"""Unit tests for sre.elasticsearch.rolling-operation."""

import importlib
from datetime import timezone
from unittest import mock

from cookbooks.sre.elasticsearch import valid_datetime_type


# The cookbook filename contains a hyphen, so normal import syntax cannot load it.
rolling_operation = importlib.import_module("cookbooks.sre.elasticsearch.rolling-operation")


def test_valid_datetime_type_treats_naive_datetime_as_utc():
    """It should support the historical timezone-naive input format."""
    dt = valid_datetime_type("2026-06-25T19:06")

    assert dt.tzinfo is timezone.utc
    assert dt.isoformat() == "2026-06-25T19:06:00+00:00"


def test_valid_datetime_type_converts_aware_datetime_to_utc():
    """It should support `date -Iseconds` style offset input."""
    dt = valid_datetime_type("2026-06-25T13:06:00-07:00")

    assert dt.tzinfo is timezone.utc
    assert dt.isoformat() == "2026-06-25T20:06:00+00:00"


def test_get_runner_uses_parsed_start_datetime():
    """It should pass timezone-aware UTC to Spicerack's node selector."""
    spicerack = mock.MagicMock()
    cookbook = rolling_operation.RollingOperation(spicerack)
    args = cookbook.argument_parser().parse_args(
        [
            "cloudelastic",
            "T426862",
            "--restart",
            "--start-datetime",
            "2026-06-25T19:06",
        ]
    )

    runner = cookbook.get_runner(args)

    assert runner.start_datetime.tzinfo is timezone.utc
    assert runner.start_datetime.isoformat() == "2026-06-25T19:06:00+00:00"


def test_get_runner_defaults_start_datetime_to_utc():
    """It should default to a timezone-aware UTC start datetime."""
    spicerack = mock.MagicMock()
    cookbook = rolling_operation.RollingOperation(spicerack)
    args = cookbook.argument_parser().parse_args(["cloudelastic", "T426862", "--restart"])

    runner = cookbook.get_runner(args)

    assert runner.start_datetime.tzinfo is timezone.utc
