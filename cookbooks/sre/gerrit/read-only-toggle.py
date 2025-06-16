"""Gerrit read-only plugin toggle cookbook.

This cookbook manages Gerrit's read-only plugin toggles.
"""

import logging
from argparse import ArgumentParser
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation
from cookbooks.sre import CookbookBase, CookbookRunnerBase
logger = logging.getLogger(__name__)


class GerritReadOnlyToggle(CookbookBase):
    """Toggles Gerrit's read-only plugin."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--host",
            required=True,
            help="Host to toggle.",
        )
        parser.add_argument(
            "--toggle",
            required=True,
            help="State to toggle to.",
            choices=["on", "off"],
        )

        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return GerritReadOnlyToggleRunner(args, self.spicerack)


class GerritReadOnlyToggleRunner(CookbookRunnerBase):
    """Runner class for executing the toggles."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 60

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()
        self.spicerack = spicerack
        self.host = spicerack.remote().query(f"{args.host}.*")
        self.message = f"from {self.host}"
        self.args = args

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def enable_gerrit_read_only_plugin(self) -> None:
        """Enables the Gerrit read-only plugin by touching a marker file."""
        self.host.run_sync(
            "touch /etc/gerrit/gerrit.readonly",
            is_safe=False,
            print_progress_bars=False,
            print_output=False,
        )

    def disable_gerrit_read_only_plugin(self) -> None:
        """Disables the Gerrit read-only plugin by removing the marker file."""
        self.host.run_sync(
            "rm -vf /etc/gerrit/gerrit.readonly",
            is_safe=False,
            print_progress_bars=False,
            print_output=True,
        )

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        if self.args.toggle == "on":
            ask_confirmation("Please confirm you want to make that instance read-only.")
            self.enable_gerrit_read_only_plugin()
        else:
            ask_confirmation("Please confirm you want to make that instance read-write.")
            self.disable_gerrit_read_only_plugin()
