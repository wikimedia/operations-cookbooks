"""Cookbook for updating wikireplica views."""
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class UpdateWikireplicaViews(CookbookBase):
    """Apply changes to the wikireplica views.

    These are defined in puppet/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml
    and the (currently manual) process is documented in
    https://wikitech.wikimedia.org/wiki/Portal:Data_Services/Admin/Wiki_Replicas#Updating_views

    Usage example:
      cookbook sre.wikireplicas.update-views --section s1 --task-id T12345
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "-t", "--task-id", help="Phabricator task ID (e.g. T123456) to log to"
        )

        # TODO eventually "all" sections could be passed, which would iterate through each section.
        parser.add_argument(
            "--section",
            choices=[
                "s1",
                "s2",
                "s3",
                "s4",
                "s5",
                "s6",
                "s7",
                "s8",
            ],
            help="Database section to be updated.",
            required=True,
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

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()

        self.username = spicerack.username
        self.actions = spicerack.actions

        self.task_id = args.task_id
        self.section = args.section

        if self.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

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

    def _determine_dbproxy_host_to_update(self, category) -> RemoteHosts:
        """Find the dbproxy host for the given category (one of analytics/web).

        Returns a RemoteHosts object which can be sent further commands.
        """
        query = f"C:profile::mariadb::proxy::multiinstance_replicas%replica_type = {category}"

        dbproxy_hosts = self.remote.query(query)

        if len(dbproxy_hosts) != 1:
            raise RuntimeError(f"Expected 1 dbproxy host, got {dbproxy_hosts.hosts}")

        return dbproxy_hosts

    def _determine_clouddb_hostname_to_update(self, section, category) -> str:
        query = f"P{{O:wmcs::db::wikireplicas::{category}_multiinstance}} and A:db-section-{section}"

        clouddb_hosts = self.remote.query(query).hosts

        if len(clouddb_hosts) != 1:
            raise RuntimeError(f"Expected 1 clouddb host, got {clouddb_hosts}")

        return clouddb_hosts[0]

    def _depool_host(self, dbproxy_host, clouddb_hostname, section):
        dbproxy_host.run_sync(
            f'echo "set server {clouddb_hostname} state drain" | socat /run/haproxy/haproxy.sock stdio'
        )

        grep_command = f"grep -qvz '^mariadb-{section},{clouddb_hostname}'"
        desired_state = "depooled"

        self._confirm_haproxy_state(
            dbproxy_host, clouddb_hostname, grep_command, desired_state
        )

    def _confirm_haproxy_state(
        self, dbproxy_host, clouddb_hostname, grep_command, desired_state
    ):
        try:
            self._retry_confirm_haproxy_state(dbproxy_host, grep_command)
            self.actions[dbproxy_host].success(
                f"Confirmed {clouddb_hostname} is {desired_state} from {dbproxy_host}"
            )
        except RemoteExecutionError:
            self.actions[dbproxy_host].failure(
                f"**Could not confirm host is {desired_state}**"
            )
            raise

    @staticmethod
    @retry(tries=5, backoff_mode="constant", exceptions=(RemoteExecutionError,))
    def _retry_confirm_haproxy_state(dbproxy_host, grep_command):
        dbproxy_host.run_sync(
            f"echo 'show stat' | socat /run/haproxy/haproxy.sock stdio | {grep_command}"
        )

    def _repool_host(self, dbproxy_host, clouddb_hostname, section):

        dbproxy_host.run_sync(
            f'echo "set server {clouddb_hostname} state ready" \
                | socat /run/haproxy/haproxy.sock stdio'
        )

        grep_command = f"grep -qz '^mariadb-{section},{clouddb_hostname}'"
        desired_state = "repooled"

        self._confirm_haproxy_state(
            dbproxy_host, clouddb_hostname, grep_command, desired_state
        )

    @staticmethod
    def _close_connections(clouddb_hostname, section):
        # TODO this can be automated; perhaps by restarting mysql
        # applications are required to handle the database disconnecting
        print(
            f"Now ssh to host {clouddb_hostname} to close any lingering database connections."
        )
        print("Connect to mariadb with the following command:")
        print(f"    sudo mysql -S /var/run/mysqld/mysqld.{section}.sock")
        print("Check the processes list in the User column")
        print("for anything started by users (u####) and services (s####):")
        print("    MariaDB [(none)]> show processlist;")
        print('Quit any remaining "userspace" connections using kill <id>')

        ask_confirmation("Have all the user connections been closed?")

    @staticmethod
    def _run_sql_test_pre():
        # TODO allow passing the sql command as an argument, like:
        #     --test-sql 'select * from enwiki.flaggedrevs limit 1'
        # which would be run like:
        #     sudo mysql -S /var/run/mysqld/mysqld.s1.sock -e 'select * from enwiki.flaggedrevs limit 1'
        print("Now is a good time to run a sql statement that you expect to fail,")
        print("with the views not having the change applied yet.")
        print("Run your test sql statement now.")

        ask_confirmation(
            "Does your sql statement reflect the expected pre-update state?"
        )

    def _run_maintain_views(self, clouddb_hostname):
        host_actions = self.actions[clouddb_hostname]

        remote_host = self.remote.query(clouddb_hostname)

        try:
            # TODO is it a reasonable approach to run against all tables and all databases?
            # It takes longer to run, but it simplifies the logic of filtering for specific
            # databases or tables.
            command = "maintain-views --all-databases --replace-all"
            remote_host.run_sync(command)
            host_actions.success(f"Ran {command}")
        except RemoteExecutionError:
            host_actions.failure(
                "**The maintain-views failed, see OUTPUT of 'maintain-views ...' above for details**"
            )
            raise

    @staticmethod
    def _run_sql_test_post():
        print(
            "Now re-run the sql statement that failed before, to test the views have been updated."
        )

        ask_confirmation("Does your sql statement reflect the expected outcome?")

    def _update_views_on_host(self, dbproxy_host, clouddb_hostname, section):
        self._depool_host(dbproxy_host, clouddb_hostname, section)

        self._close_connections(clouddb_hostname, section)
        self._run_sql_test_pre()

        self._run_maintain_views(clouddb_hostname)

        self._run_sql_test_post()
        self._repool_host(dbproxy_host, clouddb_hostname, section)

    def rollback(self):
        """Comment on phabricator in case of a failed run."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                (
                    f"Cookbook {__name__} for section {self.section} started by {self.username} executed with errors:\n"
                    f"{self.actions}\n"
                ),
            )

    def run(self):
        """Run the cookbook."""
        db_categories = ["analytics", "web"]

        if self.phabricator is not None:
            phab_message = self._format_phab_message(
                "Started updating wikireplica views"
            )
            self.phabricator.task_comment(self.task_id, phab_message)

        for category in db_categories:
            dbproxy_host = self._determine_dbproxy_host_to_update(category)
            clouddb_hostname = self._determine_clouddb_hostname_to_update(
                self.section, category
            )
            self._update_views_on_host(dbproxy_host, clouddb_hostname, self.section)

        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                (
                    f"Cookbook {__name__} for section {self.section} started by {self.username} completed:\n"
                    f"{self.actions}\n"
                ),
            )
