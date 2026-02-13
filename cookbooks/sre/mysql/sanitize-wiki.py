"""Cookbook for sanitizing new wikis in MariaDB."""

import logging
import re
from argparse import ArgumentParser

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.mysql import Instance as MInst
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


# NOTE: this scripts is written defensively. Please prioritize safety and readability,
# minimize abstractions and state, enable type checking, do assertions, write tests
# pylint: disable=missing-docstring
# pylint: disable=R0913,R0917
# flake8: noqa: D103

log = logging.getLogger(__name__)

SLUG = "cookbooks.sre.mysql.sanitize_wiki"


def ensure(condition: bool, msg: str) -> None:
    # just some syntactic sugar for readability
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def step(slug: str, msg: str) -> None:
    """
    Log next step in a friendly/greppable format.
    """
    # TODO: store the step in zarcillo/etcd to create visibility
    # around the automation process
    # TODO: also log msg in open telemetry format for tracing
    log.info("[%s.%s] %s", SLUG, slug, msg)


def run(hosts, cmd: str, is_safe=False) -> None:
    log.info(f"Running '{cmd}' on {hosts}")
    hosts._remote_hosts.run_sync(cmd, print_progress_bars=False, is_safe=is_safe)  # pylint: disable=protected-access


class SanitizeWiki(CookbookBase):
    """Manage sanitization in MariaDB for one or multiple new wiki(s).

    This cookbook unfolds what is documented here:
        https://wikitech.wikimedia.org/wiki/MariaDB/PII
    cookbook sre.mysql.sanitize-wiki --wiki myfirstwiki --wiki mysecondwiki
        Will sanitize those wikis according to what is documented above.
    cookbook sre.mysql.sanitize-wiki --wiki mywiki --check-only
        This will run the first part of the procedure,
        limited to checking if there is some sanitization to perform.
    """

    argument_task_required = True

    def argument_parser(self) -> ArgumentParser:
        """Define CLI arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--wiki",
            required=True,
            action="append",
            help="Name of the wiki (can be repeated)",
        )
        group = parser.add_mutually_exclusive_group()
        group.add_argument(
            "--check-only",
            action="store_true",
            help="Only perform checks without making changes",
        )
        help_text = """Only perform grant permissions and create view database. This should be quicker than a \
            typical run. It will skip checking existing needs for sanitization in dbs, and focus on the last steps \
            as in https://wikitech.wikimedia.org/wiki/MariaDB/PII#Grant_Permissions_for_SQL_Views"""
        group.add_argument(
            "--only-grant-and-view",
            action="store_true",
            help=help_text,
        )
        # we are only adding wikis to s5 for the foreseeable future
        group.add_argument("--section", default="s5", help="Section (defaults to s5)")
        return parser

    def get_runner(self, args):
        """Get the runner for this cookbook."""
        return SanitizeWikiRunner(args, self.spicerack)


class SanitizeWikiRunner(CookbookRunnerBase):
    """Runner for sanitization cookbook."""

    def __init__(self, args, spicerack: Spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self._wiki_names: list[str] = args.wiki
        ensure(re.fullmatch(r"s\d", args.section) is not None, f"Invalid section name '{args.section}'")
        self.section = args.section
        self._san_sock_fn = f"/run/mysqld/mysqld.{self.section}.sock"
        self.mysql = spicerack.mysql()
        self.check_only = args.check_only
        self.only_grant_and_view = args.only_grant_and_view
        self.logger = logging.getLogger(__name__)
        self.clouddb_hosts = self.mysql.get_dbs("A:db-clouddb-sanitization")
        self.sanitarium_hosts = self.mysql.get_dbs(f"A:db-sanitarium and A:db-section-{self.section}")
        self.logger.info("Sanitarium hosts: %s", self.sanitarium_hosts)
        self._task_id = args.task_id
        self._admin_reason = spicerack.admin_reason(f"Sanitize wiki {self.section}", task_id=self._task_id)
        self._phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    @property
    def lock_args(self):
        """Lock per-section"""
        return LockArgs(suffix=self.section, concurrency=1, ttl=3600)

    @property
    def runtime_description(self) -> str:
        """Return a description of the cookbook's action."""
        if self.check_only:
            action = "Checking"
        elif self.only_grant_and_view:
            action = "Setting up permissions and view database"
        else:
            action = "Managing"

        wns = ", ".join(self._wiki_names)
        return f"{action} sanitization for wikis {wns} in section {self.section}"

    def run(self) -> None:
        """Execute the sanitization process."""
        if not self.check_only:
            step("clean_up", "Cleaning up data on sanitarium hosts")
            for wiki in self._wiki_names:
                self.logger.info("Processing wiki: %s", wiki)
                self._redact_sanitarium_data(wiki)

            wikis = ", ".join(self._wiki_names)
            self._update_phab(f"Wikis {wikis} redacted")

            if not self.only_grant_and_view:
                self._check_and_drop_private_data_on_hosts(clouddbs=False)

        self._check_and_drop_private_data_on_hosts(clouddbs=True)

        if not self.check_only:
            step("grant_and_view", "Grant permissions and create view database on clouddb")
            for wiki in self._wiki_names:
                self._setup_clouddb(wiki)

            wikis = ", ".join(self._wiki_names)
            self._update_phab(f"Wikis {wikis} set up on clouddb")

        self.logger.info("Sanitization completed for all wikis")

    def _redact_sanitarium_data(self, wiki_name: str) -> None:
        command = (
            f"/usr/local/sbin/redact_sanitarium.sh -d {wiki_name} -S {self._san_sock_fn} | "
            f"/usr/local/bin/mysql -S {self._san_sock_fn}"
        )
        if self.only_grant_and_view:
            self.logger.info("Skipping redacting for %s", wiki_name)
            return

        run(self.sanitarium_hosts, command)

    def _check_and_drop_private_data_on_hosts(self, clouddbs=False) -> None:
        step("check_priv", "Check private data on sanitarium")
        check_command = f"/usr/local/sbin/check_private_data.py -S {self._san_sock_fn}"
        run(self.sanitarium_hosts, check_command, is_safe=True)

        if not self.check_only:
            ask_confirmation("Proceed with dropping private data?")
            step("drop_privdata", "Drop private data")
            drop_command = f"{check_command} | /usr/local/bin/mysql -S {self._san_sock_fn}"
            run(self.sanitarium_hosts, drop_command)

        if not clouddbs:
            self.logger.info("Skipping checking on clouddbs.")
            return

        if not self.only_grant_and_view:
            step("check", "Check private data on clouddb")
            run(self.clouddb_hosts, check_command)

    def _setup_clouddb(self, wiki_name: str) -> None:
        """Set up permissions and view database on clouddb hosts for a specific wiki."""
        create_db_query = f"CREATE DATABASE IF NOT EXISTS {wiki_name}_p;"
        grant_query = f"GRANT SELECT, SHOW VIEW ON {wiki_name}_p.* TO labsdbuser;"
        for host in self.clouddb_hosts.remote_hosts:
            instance = MInst(host, name=self.section)
            self.logger.info("Running '%s' on %s on %s", create_db_query, instance, host)
            instance.run_query(create_db_query, is_safe=False)
            self.logger.info("Running '%s' on %s on %s", grant_query, instance, host)
            instance.run_query(grant_query, is_safe=False)

    def _update_phab(self, msg: str) -> None:
        msg = f"Section {self.section}: {msg} - {self._admin_reason.owner}"
        self._phab.task_comment(self._task_id, msg)
