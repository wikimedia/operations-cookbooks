"""Switch Datacenter specific steps for Databases."""
import logging
from abc import abstractmethod
from collections.abc import Sequence

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.mysql import CORE_SECTIONS, Instance
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

        if args.section:
            self.sections: Sequence[str] = (args.section,)
            self.description_suffix = f"section {args.section}"
        else:
            self.sections = CORE_SECTIONS
            self.description_suffix = "all core sections"

        self.dry_run = spicerack.dry_run
        self.mysql = spicerack.mysql()
        self.remote = spicerack.remote()
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.actions = spicerack.actions
        self.reason = spicerack.admin_reason(
            f"Databases pre/post steps for DC Switchover {self.dc_from} -> {self.dc_to}",
            task_id=self.task_id)
        self.phab_prefix = f"{self.__module__} {self.runtime_description}"

    @property
    def runtime_description(self):
        """Return the decription to use in SAL."""
        return f"for the switch from {self.dc_from} to {self.dc_to} for {self.description_suffix}"

    def rollback(self):
        """Save the current actions to Phabricator."""
        self.phabricator.task_comment(
            self.task_id, f"{self.phab_prefix} executed by {self.reason.owner} with errors:\n{self.actions}")

    def get_remote_masters(self, section: str):
        """Get the RemoteHosts instance for the from/to masters based on the section."""
        if section in CORE_SECTIONS:
            remote_master_from = self.mysql.get_core_dbs(
                datacenter=self.dc_from, section=section, replication_role="master")
            remote_master_to = self.mysql.get_core_dbs(
                datacenter=self.dc_to, section=section, replication_role="master")
        else:
            base_query = f"P{{R:profile::mariadb::section%section = {section}}} and A:db-role-master and"
            remote_master_from = self.mysql.get_dbs(f"{base_query} A:{self.dc_from}")
            remote_master_to = self.mysql.get_dbs(f"{base_query} A:{self.dc_to}")

        return remote_master_from, remote_master_to

    def run(self):
        """As required by Spicerack API."""
        self.phabricator.task_comment(self.task_id, f"{self.phab_prefix} started by {self.reason.owner}")

        for i, section in enumerate(self.sections, start=1):
            num_sections = len(self.sections)
            logger.info("==> [%d/%d] Performing steps for section %s", i, num_sections, section)
            remote_master_from, remote_master_to = self.get_remote_masters(section)
            master_from = remote_master_from.list_hosts_instances()[0]
            master_to = remote_master_to.list_hosts_instances()[0]
            logger.info("Found masters for DC_FROM %s and DC_TO %s for section %s",
                        master_from.host, master_to.host, section)

            ask_confirmation(f"[{i}/{num_sections}] Ready to run on section {section}, ok to proceed?")
            try:
                self.run_on_section(section, master_from, master_to)
                self.phabricator.task_comment(
                    self.task_id,
                    f"{self.phab_prefix} run successfully on section {section}:\n{self.actions[section]}",
                )
            except AbortError:
                self.actions[section].failure("**Execution for this section was manually aborted**")
                self.phabricator.task_comment(
                    self.task_id,
                    f"{self.phab_prefix} was aborted for section {section}:\n{self.actions[section]}",
                )
                if i < len(self.sections):
                    num = len(self.sections) - i
                    ask_confirmation(f"Run on section {section} was manually aborted. "
                                     f"Continue with the remaining {num} sections or abort completely?")

        self.phabricator.task_comment(
            self.task_id, f"{self.phab_prefix} executed by {self.reason.owner} completed.")

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
            "--section",
            help=("Run only for a specific section. Works also for non-core sections. The specified name will be used "
                  "in this PuppetDB query to select the target hosts and then filtered by role and datacenter: "
                  "R:profile::mariadb::section%%section = SECTION_NAME"),  # % need to be escaped
        )
        parser.add_argument(
            "dc_from", metavar="DC_FROM", choices=CORE_DATACENTERS,
            help=(
                "Name of the datacenter switching away from. One of: %(choices)s. "
                "This refers to the whole DC swithover process, so for finalize it must be the old primary."
            ),
        )
        parser.add_argument(
            "dc_to", metavar="DC_TO", choices=CORE_DATACENTERS,
            help=(
                "Name of the datacenter switching to. One of: %(choices)s. "
                "This refers to the whole DC swithover process, so for finalize it must be the new primary."
            ),
        )
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        if args.dc_from == args.dc_to:
            raise ValueError(f"DC_FROM ({args.dc_from}) and DC_TO ({args.dc_to}) must differ")

        return self.runner_class(args, self.spicerack)
