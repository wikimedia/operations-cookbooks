import logging
from argparse import ArgumentError
from importlib import import_module

from pytest import (
    fixture,
    raises,
)


@fixture(autouse=True)
def set_logging(caplog):
    caplog.set_level(logging.DEBUG)
    caplog.handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))


noop_cookbook = import_module("cookbooks.sre.backup.noop")


def test_parser_args(mocker, caplog):
    assert noop_cookbook.Noop

    m_spicerack = mocker.MagicMock(name="Spicerack")
    tc = noop_cookbook.Noop(spicerack=m_spicerack)

    assert tc.argument_reason_required is False
    assert tc.argument_task_required is False

    parser = tc.argument_parser()
    options = parser.parse_args([
        "--debug",
        "*.eqiad.wmnet",
        "noop",
    ])
    runner = tc.get_runner(options)

    assert options.action == "noop"
    assert isinstance(runner, noop_cookbook.NoopRunner)
    assert runner.args.debug

    with caplog.at_level(logging.INFO, logger=runner.log.name):
        runner.debug("DEBUG MODE ENABLED")
    assert "DEBUG MODE ENABLED" in caplog.text

    # python 3.13 ArgumentParser.parse_args allows use of self.exit_on_error to bypass SystemExit.
    #             However, that does not seem consistent for all possible scenarios, so allowing
    #             SystemExit to be raised. This avoids the need to check versions too.
    tc.argument_reason_required = True
    parser = tc.argument_parser()
    parser.exit_on_error = False
    with raises((ArgumentError, SystemExit)):
        parser.parse_args([
            "*.eqiad.wmnet",
            "noop",
        ])

    tc.argument_reason_required = False
    tc.argument_task_required = True
    parser = tc.argument_parser()
    parser.exit_on_error = False
    with raises((ArgumentError, SystemExit)):
        parser.parse_args([
            "*.eqiad.wmnet",
            "noop",
        ])
