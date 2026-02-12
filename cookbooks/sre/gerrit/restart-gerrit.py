"""Gerrit restart cookbook.

This cookbook manages the restart of the Gerrit service on a specific host
with appropriate downtime handling.
"""

import logging
import time
from argparse import ArgumentParser

from spicerack.alertmanager import AlertmanagerError
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)

TARGET_ALERTS = "GerritHAProxyServiceUnavailable|GerritHAProxyBackendUnavailable"


class Restart(CookbookBase):
    """Restarts Gerrit service on a target host."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--host",
            required=True,
            help="Host where the Gerrit service needs to be restarted",
        )
        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return RestartRunner(args, self.spicerack)


class RestartRunner(CookbookRunnerBase):
    """Runner class for executing Gerrit restart."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 900  # 15 minutes should be enough for a restart

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.args = args

        self.target_host = spicerack.host(args.host)

        self.message = f"Restarting Gerrit on {args.host}"
        self.reason = self.spicerack.admin_reason(reason=self.message)

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def _restart_service(self):
        """Helper to execute the actual restart logic."""
        logger.info("Restarting gerrit service on %s", self.args.host)
        self.target_host.remote().run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False,
            print_output=True,
            is_safe=False
        )
        if not self.spicerack.dry_run:
            logger.info("Waiting 60 seconds for monitoring to catch up...")
            time.sleep(60)
        else:
            logger.info("Skipping monitoring wait because of dry run.")

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alertmanager = self.spicerack.alertmanager()

        matchers = [
            {'name': 'alertname', 'value': TARGET_ALERTS, 'is_regex': True}
        ]

        logger.info("Setting downtime for %s", self.args.host)
        with self.target_host.alerting().downtimed(self.reason):
            try:
                with alertmanager.downtimed(reason=self.reason, matchers=matchers):
                    ask_confirmation(
                        f"About to restart Gerrit on {self.args.host}. "
                        "Full downtime active (Host + Alertmanager). Proceed?"
                    )
                    self._restart_service()

            except AlertmanagerError as e:
                logger.warning("Exception thrown downtiming alerts. Reason: %s", e)

                ask_confirmation(
                    f"WARNING: Failed to issue downtime for: {TARGET_ALERTS} "
                    "Proceed anyway?"
                )
                self._restart_service()

        logger.info("Gerrit restart completed successfully. Downtimes removed.")
