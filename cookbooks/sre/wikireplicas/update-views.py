"""Cookbook for updating wikireplica views."""
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable
from wmflib.phabricator import Phabricator

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class UpdateWikireplicaViews(CookbookBase):
    """Apply changes to the wikireplica views.

    These are defined in puppet/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml
    and the (currently manual) process is documented in
    https://wikitech.wikimedia.org/wiki/Portal:Data_Services/Admin/Wiki_Replicas#Updating_views

    Usage example:
      cookbook sre.wikireplicas.update-views --task-id T12345
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--clean",
            action="store_true",
            help="Drop views that are no longer present in the yaml file. "
            "This option is not compatible with --table."
        )
        group.add_argument(
            "--table", help="Only update the specified table (e.g. --table globaluser)"
        )
        parser.add_argument(
            "--database", help="Only update the specified database (e.g. --database centralauth)"
        )
        parser.add_argument(
            "-t", "--task-id", help="Phabricator task ID (e.g. T123456) to log to"
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UpdateWikireplicaViewsRunner(args, self.spicerack)


class UpdateWikireplicaViewsRunner(CookbookRunnerBase):
    """Wikireplica views updater cookbook runner class"""

    def _format_phab_message(self, message):
        """Give context to a message to be posted to phabricator."""
        cookbook_name = __name__
        return f"Cookbook {cookbook_name} run by {self.username}: {message}"

    def __init__(self, args, spicerack: Spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.username = spicerack.username
        self.actions = spicerack.actions

        self.clean = args.clean
        self.table = args.table
        self.database = args.database
        self.task_id = args.task_id

        self.phabricator: Optional[Phabricator] = None
        if self.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        self.remote = spicerack.remote()

        self._ensure_notify_dbas()
        self._ensure_view_definitions_updated()

    @staticmethod
    def _ensure_view_definitions_updated():
        print(
            "If maintain-views.yaml has changed and there is a puppet patch open, submit and puppet-merge it now."
        )
        ask_confirmation("Have any relevant changes been `puppet-merge`d?")

    def _ensure_notify_dbas(self):
        print(
            "Post in #wikimedia-data-persistence that you are updating the wikireplicas views."
        )
        if self.task_id:
            print(f"Include the task number {self.task_id}")
        else:
            print("Include the task number.")

        ask_confirmation("Ready to proceed?")

    @staticmethod
    def _run_sql_test_pre(host: RemoteHosts):
        # TODO allow passing the sql command as an argument, like:
        #     --test-sql 'select * from enwiki.flaggedrevs limit 1'
        # which would be run like:
        #     sudo mysql -S /var/run/mysqld/mysqld.s1.sock -e 'select * from enwiki.flaggedrevs limit 1'
        print("Now is a good time to run a sql statement that you expect to fail,")
        print("with the views not having the change applied yet.")
        print("Run your test sql statement now.")

        ask_confirmation(
            f"Does your SQL statement reflect the expected pre-update state on {host}?"
        )

    def _run_maintain_views_on_host(self, remote_hosts: RemoteHosts):
        host_actions = self.actions[str(remote_hosts)]

        maintain_views_options = "--replace --auto-depool"

        if self.clean:
            maintain_views_options += " --clean"

        if self.database:
            maintain_views_options += f" --databases {self.database}"
        else:
            maintain_views_options += " --all-databases"

        if self.table:
            maintain_views_options += f" --table {self.table}"

        try:
            command = f"maintain-views {maintain_views_options}"
            remote_hosts.run_sync(command)
            host_actions.success(f"Ran '{command}'")
        except RemoteExecutionError:
            host_actions.failure(
                "**The maintain-views run failed, see OUTPUT of 'maintain-views ...' above for details**"
            )
            raise

    def _run_maintain_views(self, remote_hosts: RemoteHosts):
        self.spicerack.puppet(remote_hosts).run()
        for host in remote_hosts.split(len(remote_hosts)):
            self.actions[str(host)].success("Ran Puppet agent")
            self._run_maintain_views_on_host(host)

    @staticmethod
    def _run_sql_test_post(host: RemoteHosts):
        print(
            "Now re-run the sql statement that failed before, to test the views have been updated."
        )

        ask_confirmation(f"Does your SQL statement reflect the expected outcome on {host}?")

    def _test_with_dedicated_host(self, query: str):
        """Interactively check that the change looks fine on the special dedicated analytics host."""
        remote_hosts = self.remote.query(query)
        self._run_sql_test_pre(remote_hosts)
        self._run_maintain_views(remote_hosts)
        self._run_sql_test_post(remote_hosts)

    def rollback(self):
        """Comment on phabricator in case of a failed run."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                (
                    f"Cookbook {__name__} started by {self.username} executed with errors:\n"
                    f"{self.actions}\n"
                ),
            )

    def run(self):
        """Run the cookbook."""
        if self.phabricator is not None:
            phab_message = self._format_phab_message("Started updating wiki replica views")
            self.phabricator.task_comment(self.task_id, phab_message)

        self._test_with_dedicated_host("O:wmcs::db::wikireplicas::dedicated::analytics_multiinstance")

        for category in ["analytics", "web"]:
            self._run_maintain_views(self.remote.query(f"P{{O:wmcs::db::wikireplicas::{category}_multiinstance}}"))

        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                (
                    f"Cookbook {__name__} started by {self.username} completed:\n"
                    f"{self.actions}\n"
                ),
            )
