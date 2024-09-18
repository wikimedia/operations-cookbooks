"""Switch Datacenter specific steps for Databases."""
import logging
from abc import abstractmethod

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.mysql_legacy import CORE_SECTIONS, Instance
from wmflib.constants import CORE_DATACENTERS
from wmflib.interactive import AbortError, ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


__title__ = __doc__
logger = logging.getLogger(__name__)


class DatabaseRunnerBase(CookbookRunnerBase):
    """As required by Spicerack API."""

    lock_ttl = 3600
    max_concurrency = 1

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()

        self.dc_from = args.dc_from
        self.dc_to = args.dc_to
        self.task_id = args.task_id

        self.dry_run = spicerack.dry_run
        self.mysql = spicerack.mysql_legacy()
        self.remote = spicerack.remote()
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.actions = spicerack.actions
        self.reason = spicerack.admin_reason(
            f"Databases pre/post steps for DC Switchover {self.dc_from} -> {self.dc_to}",
            task_id=self.task_id)
        self.phab_prefix = f"{self.__module__} {self.runtime_description} started by {self.reason.owner}"

    @property
    def runtime_description(self):
        """Return the decription to use in SAL."""
        return f"for the switch from {self.dc_from} to {self.dc_to}"

    def rollback(self):
        """Save the current actions to Phabricator."""
        self.phabricator.task_comment(self.task_id, f"{self.phab_prefix} executed with errors:\n{self.actions}")

    def run(self):
        """As required by Spicerack API."""
        self.phabricator.task_comment(self.task_id, self.phab_prefix)

        for section in CORE_SECTIONS:
            logger.info("==> Performing steps for section %s", section)
            remote_master_from = self.mysql.get_core_dbs(
                datacenter=self.dc_from, section=section, replication_role="master")
            remote_master_to = self.mysql.get_core_dbs(
                datacenter=self.dc_to, section=section, replication_role="master")

            master_from = remote_master_from.list_hosts_instances()[0]
            master_to = remote_master_to.list_hosts_instances()[0]
            logger.info("Found masters for DC_FROM %s and DC_TO %s for section %s",
                        master_from.host, master_to.host, section)

            ask_confirmation(f"Ready to run on section {section}, ok to proceed?")
            try:
                self.run_on_section(section, master_from, master_to)
            except AbortError:
                self.actions[section].failure("**Execution for this section was manually aborted**")
                ask_confirmation(f"Run on section {section} was manually aborted. "
                                 "Continue with the remaining sections or abort completely?")

        self.phabricator.task_comment(self.task_id, f"{self.phab_prefix} completed:\n{self.actions}")

    @abstractmethod
    def run_on_section(self, section: str, master_from: Instance, master_to: Instance):
        """Run all the steps on a given section."""


class DatabaseCookbookBase(CookbookBase):
    """Base class to be inherited by all the Database related cookbooks for the datacenter switchover."""

    # To be overwritten by derived class with their own class derived from DatabaseRunnerBase
    runner_class: type[DatabaseRunnerBase]

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("-t", "--task-id", required=True,
                            help="the Phabricator task ID to update and refer (i.e.: T12345)")
        parser.add_argument(
            "dc_from", metavar="DC_FROM", choices=CORE_DATACENTERS,
            help=(
                "Name of the datacenter switching away from. One of: %(choices)s. "
                "This refers to the whole DC swithover process, so for finalize it must be the old primary."
            )
        )
        parser.add_argument(
            "dc_to", metavar="DC_TO", choices=CORE_DATACENTERS,
            help=(
                "Name of the datacenter switching to. One of: %(choices)s. "
                "This refers to the whole DC swithover process, so for finalize it must be the new primary."
            )
        )
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        if args.dc_from == args.dc_to:
            raise ValueError(f"DC_FROM ({args.dc_from}) and DC_TO ({args.dc_to}) must differ")

        return self.runner_class(args, self.spicerack)
