"""Noop cookbook."""

from argparse import (
    ArgumentParser,
    Namespace,
)
#from wmflib.interactive import ask_confirmation

from cookbooks.sre.backup import (
    BackupCookbookBase,
    BackupCookbookRunnerBase,
)


class Noop(BackupCookbookBase):
    """Test cluster."""

    argument_reason_required = False
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()

        actions = parser.add_subparsers(
            dest="action",
            required=True,
            title="actions",
            help="Available actions",
        )

        # noop action
        _ = actions.add_parser(
            "noop",
            help="Nothing to do here, move along.",
        )

        # echo action
        _ = actions.add_parser(
            "echo",
            help="echo nothing.",
        )

        return parser

    def get_runner(self, args: Namespace) -> "NoopRunner":
        """As specified by Spicerack API."""
        return NoopRunner(args, self.spicerack)


class NoopRunner(BackupCookbookRunnerBase):
    """Noop cookbook runner."""

    @property
    def max_hosts(self) -> int:
        """Control the maximum host count for the runner.

        Provide per-cookbook control over the acceptable host count for a run.

        0 - use to provide unlimited hosts
        1 - this should be the default, limiting execution to a single host
        n - a positive integer greater than 1 should be used with care
        """
        return 0

    @property
    def permitted_sections(self) -> list:
        """Provide a list of permitted sections."""
        return ["A:bacula", "A:db-backups", "A:mediabackups"]

    def echo(self) -> None:
        """Echo nothing"""
        self.run_sync_all(command="echo -n", ensure_green=False)

    def noop(self) -> None:
        """Echo nothing"""
        for host in self.hosts:
            pass

    def run(self) -> None:
        """Required by the Spicerack API."""
        super().run()

        match self.args.action:
            case "noop" | "echo":
                self.check_sections()
                getattr(self, self.args.action)()
                self.debug("Doing nothing with more noise!")
            case _:
                raise ValueError(f"Invalid action chosen ({self.args.action}")

