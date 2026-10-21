"""Backup cookbooks.

Shared library-functionality is present in this file
to help reorganise and reduce duplication. Most should
be moved to spicerack.backup, once mature.
"""

__owner_team__ = "Data Persistence"

import logging
from abc import (
    ABCMeta,
    abstractmethod,
)
from argparse import (
    ArgumentParser,
    Namespace,
)
from enum import Enum
from os import getenv

from spicerack import Spicerack
from spicerack.cookbook import (
    CookbookBase,
    CookbookRunnerBase,
)
from spicerack.remote import (
    RemoteError,
    RemoteExecutionError,
    RemoteHosts,
)
from wmflib.interactive import (
    AbortError,
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
)

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


class YesNo(Enum):
    """Truthy type."""

    NO = False
    YES = True

    @classmethod
    def _missing_(cls, value):
        """Called automatically when value doesn't match a member's raw value."""
        if isinstance(value, str):
            value = value.strip().lower()
        if value in (1, "1", "y", "yes", "true"):
            return cls.YES
        if value in (0, "0", "n", "no", "false"):
            return cls.NO
        return None  # Enum turns this into a ValueError for us

    def __bool__(self) -> bool:
        return self.value

    def __str__(self) -> str:
        return self.name.lower()


class TooManyBackupHostsError(ValueError):
    """Indicate that the number of backup hosts is exceeding a threshold"""


class BackupCookbookBase(CookbookBase, metaclass=ABCMeta):
    """Base cookbook class for sre.backup."""

    argument_reason_required = True
    argument_task_required = True

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Enable debugging just for the cookbook.",
        )
        parser.add_argument(
            "query",
            help="Cumin query to match the host(s) to act upon.",
        )
        return parser


class BackupCookbookRunnerBase(CookbookRunnerBase, metaclass=ABCMeta):
    """Base cookbook runner class for sre.backup."""

    CAUTIOUS_MODE: YesNo = YesNo(getenv("COOKBOOK_SRE_BACKUP_CAUTIOUS_MODE", "1"))

    hosts: RemoteHosts
    log: logging.Logger

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Set things up"""
        ensure_shell_is_durable()

        self.spicerack = spicerack

        self.args = args
        self.hosts = self.get_remotehosts()
        self.icinga_hosts = spicerack.icinga_hosts
        self.alerting_hosts = spicerack.alerting_hosts
        self.log = logging.getLogger(__name__)
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.puppet = spicerack.puppet

    @property
    @abstractmethod
    def max_hosts(self) -> int:
        """Control the maximum host count for the runner.

        Provide per-cookbook control over the acceptable host count for a run.

        0 - use to provide unlimited hosts
        1 - this should be the default, limiting execution to a single host
        n - a positive integer greater than 1 should be used with care
        """

    @property
    @abstractmethod
    def permitted_sections(self) -> list:
        """Provide a list of permitted sections."""

    @property
    def permitted_section_filter(self) -> str:
        """Create a cumin filter for the permitted sections"""
        if not self.permitted_sections:
            return ""
        return f" and ({' or '.join(self.permitted_sections)})"

    def debug(self, message, **kwargs) -> None:
        """Custom debug messaging"""
        if not self.args.debug:
            return
        self.log.info("DEBUG:: %s", message, **kwargs)

    def ensure(self, condition: bool, msg: str, error_cls: type[Exception] = AssertionError) -> None:
        """Just some syntactic sugar for readability."""
        if condition:
            return
        self.log.error("Failed safety check: %s", msg, exc_info=True)
        raise error_cls(msg)

    def get_remotehosts(self) -> RemoteHosts:
        """Returns a RemoteHosts matching the query"""
        rhs: RemoteHosts | None = None
        remote_err: RemoteError | None = None

        for query in (self.args.query, f"{self.args.query}*"):
            try:
                rhs = self.spicerack.remote().query(
                    "P{" + query + "} and A:backup" + self.permitted_section_filter
                )
            except RemoteError as err:
                remote_err = err
            else:
                break

        if rhs is None:
            raise remote_err or UserWarning("Unexpected state, rhs is None and so is remote_err")

        if self.max_hosts != 0:
            self.ensure(
                len(rhs) <= self.max_hosts,
                f"{len(rhs)} remote instances found, expected {self.max_hosts} or less.",
            )
        return rhs

    def check_sections(self) -> bool:
        """Check that the chosen hosts are all in the permitted sections

        Raises: spicerack.remote.RemoteError if failing to match

        """
        self.log.info("Hosts = %s", self.hosts)
        if self.args.debug:  # Saving iteration for debugging
            for host in self.hosts:
                self.debug(f"Host: {host}")
        return len(self.hosts) > 0

    def run(self) -> None:
        """Required by the Spicerack API."""
        logging.getLogger("conftool").setLevel(logging.WARNING)

    def run_sync_single(self, host, command: str, ensure_green: bool = True) -> None:
        """Run a synchronous command across a single host."""
        self.ensure(len(host.hosts) == 1, "A single backup instance is required", TooManyBackupHostsError)
        try:
            if bool(self.CAUTIOUS_MODE):
                ask_confirmation(f"Proceed running {command} against {host}?")
            self.log.info("Executing %s on %s", command, host)
            confirm_on_failure(host.run_sync, command)
        except RemoteExecutionError as err:
            for _, mt in err.results:
                self.log.debug("Unexpected output: %s", mt)
            self.log.error("Aborting due to unexpected output")
            raise AbortError from err
        except AbortError:
            self.log.error("%s: execution aborted", command)
            raise
        if ensure_green:
            self.icinga_hosts(host.hosts).wait_for_optimal()

    def run_sync_all(self, command: str, ensure_green: bool = True) -> None:
        """Run a synchronous command across all hosts."""
        for host in self.hosts:
            self.run_sync_single(host=host, command=command, ensure_green=ensure_green)
