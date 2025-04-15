"""Gerrit failover cookbook.

This module defines a cookbook to manage Gerrit failover operations between two hosts.
"""

import logging
import re
from time import sleep
from datetime import timedelta
from argparse import ArgumentParser

from spicerack.decorators import retry
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation
from cookbooks.sre import CookbookBase, CookbookRunnerBase, PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class Failover(CookbookBase):
    """Performs a failover from one Gerrit host to another."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--switch-from-host",
            required=True,
            help="Host that we want to switch away from",
        )
        parser.add_argument(
            "--switch-to-host",
            required=True,
            help="Host that we want to switch to",
        )

        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return FailoverRunner(args, self.spicerack)


class FailoverRunner(CookbookRunnerBase):
    """Runner class for executing Failover."""

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.switch_from_host = spicerack.remote().query(f"{args.switch_from_host}.*")
        self.switch_to_host = spicerack.remote().query(f"{args.switch_to_host}.*")
        self.message = f"from {self.switch_from_host} to {self.switch_to_host}"

        self.phabricator = (
            spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        )

        self.reason = self.spicerack.admin_reason(reason=self.message)

        self.confirm_before_proceeding()

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alerting_hosts = self.spicerack.alerting_hosts(
            self.switch_from_host.hosts | self.switch_to_host.hosts
        )
        alerting_hosts.downtime(self.reason, duration=timedelta(hours=4))
        self.spicerack.puppet(self.switch_to_host).disable(self.reason)
        ask_confirmation(
            "Run sudo -i authdns-update on ns0.wikimedia.org, review the diff but **do not commit yet.**"
        )

        self.sync_files()
        self.switch_from_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False, print_output=True
        )
        self.switch_to_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False, print_output=True
        )
        # TODO offer a landing page either through Gerrit itself or through a http server
        self.spicerack.puppet(self.switch_from_host).disable(self.reason)
        self.sync_files(idempotent=True)
        ask_confirmation(
            f"Please merge the change to set the DNS records for Gerrit primary on {self.switch_to_host}. "
            "I will pause for 3 minutes to let them refresh everywhere once you hit go."
        )
        if not self.spicerack.dry_run:
            sleep(180)
        ask_confirmation(
            f"Please merge the change to set the puppet role for Gerrit primary on {self.switch_to_host}. "
            "When you hit go, we will re-enable puppet and execute a puppet run."
        )
        self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)
        self.switch_to_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True
        )
        ask_confirmation(
            "Please verify that the switchover to gerrit.wikimedia.org is operating as expected. "
            f"Once you are certain please merge the change to set the puppet role for {self.switch_from_host}, "
            "and we will re-enable and run puppet."
        )
        self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)
        self.switch_from_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True
        )
        ask_confirmation(
            "Please verify that the switchover to gerrit-replica.wikimedia.org is also operating as expected, "
            "it should return a 404 on /. If needed, please follow the remaining guidelines "
            "listed here: https://w.wiki/DoeG"
        )

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct."""
        ask_confirmation(
            f"This will migrate gerrit.wikimedia.org to {self.switch_to_host}. "
            "Check that this is definitely what you want to do."
        )

    @retry(
        tries=10,
        delay=timedelta(seconds=3),
        backoff_mode="constant",
        failure_message="Waiting for rsync to be idempotent",
        exceptions=(RuntimeError,),
    )
    def sync_files(self, idempotent=False) -> bool:
        """Transfers files from old to new Gerrit host."""
        logger.info("Starting to rsync to %s.", self.switch_to_host)
        command_sync_var_lib = (
            "/usr/bin/rsync -avpPz --stats --delete /var/lib/gerrit2/review_site/ "
            f"rsync://{self.switch_to_host}/gerrit-var-lib/"
        )
        command_sync_data = (
            "/usr/bin/rsync -avpPz --stats --delete /srv/gerrit/ "
            f"rsync://{self.switch_to_host}/gerrit-data/ --exclude=*.hprof"
        )
        transfers = [
            self.switch_from_host.run_sync(
                command_sync_var_lib,
                print_progress_bars=False, print_output=False
            ),
            self.switch_from_host.run_sync(
                command_sync_data,
                print_progress_bars=False, print_output=False
            )
        ]

        if idempotent and not self.spicerack.dry_run:
            ret = [
                self.rsync_no_changes(list(t)[0][1].message().decode("utf-8")) for t in transfers
            ]
            if all(ret):
                return True
            raise RuntimeError

        return True

    def rsync_no_changes(self, rsync_output: str) -> bool:
        """Check if rsync reports no changes in the transferred data."""
        patterns_expected_zero = {
            "Number of created files": 0,
            "Number of deleted files": 0,
            "Number of regular files transferred": 0,
            "Total transferred file size": 0,
        }

        for label, expected in patterns_expected_zero.items():
            pattern = rf"{re.escape(label)}:\s+(\d+)"
            match = re.search(pattern, rsync_output)
            if not match:
                return False
            value = int(match.group(1))
            if value != expected:
                return False

        return True
