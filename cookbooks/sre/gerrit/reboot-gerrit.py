"""Gerrit reboot cookbook.

This cookbook manages the reboot of a specific Gerrit host
with appropriate downtime handling.
"""

import logging
from argparse import ArgumentParser

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)

TARGET_ALERTS = "GerritHAProxyServiceUnavailable|GerritHAProxyBackendUnavailable"


class Reboot(CookbookBase):
    """Reboots a Gerrit host via sre.hosts.reboot-single."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--host",
            required=True,
            help="Gerrit host to reboot",
        )
        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return RebootRunner(args, self.spicerack)


class RebootRunner(CookbookRunnerBase):
    """Runner class for executing Gerrit reboot."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 1800  # 30 minutes should be enough for a single-host reboot

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.args = args

        self.target_host = spicerack.host(args.host)

        self.message = f"Rebooting Gerrit on {args.host}"
        self.reason = self.spicerack.admin_reason(reason=self.message, task_id=self.args.task_id)
        self.puppetserver = spicerack.puppet_server()
        self.cluster_primary = self.puppetserver.hiera_lookup(
            self.target_host.fqdn,
            "profile::gerrit::active_host",
        ).splitlines()[-1].strip()

        if self.cluster_primary != self.target_host.fqdn:
            raise RuntimeError(
                f"{self.args.host} is not the primary Gerrit host "
                f"(primary: {self.cluster_primary}). "
                "Please use the following command instead: "
                f"sudo cookbook sre.hosts.reboot-single {self.target_host.fqdn.split('.')[0]}"
            )

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message"""
        return self.message

    def _reboot_host(self):
        logger.info("Rebooting Gerrit host %s via sre.hosts.reboot-single", self.args.host)
        reboot_args = [self.args.host, "--reason", self.message]
        if self.args.task_id is not None:
            reboot_args.extend(["--task-id", self.args.task_id])

        self.spicerack.run_cookbook("sre.hosts.reboot-single", reboot_args, raises=True)

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alertmanager = self.spicerack.alertmanager()

        matchers = [
            {'name': 'alertname', 'value': TARGET_ALERTS, 'isRegex': True}
        ]

        logger.info("Setting downtime for %s", self.args.host)

        with alertmanager.downtimed(reason=self.reason, matchers=matchers):
            ask_confirmation(
                f"About to reboot Gerrit on {self.args.host}. "
                "Full downtime active (Host + Alertmanager). Proceed?"
            )
            self._reboot_host()

        logger.info("Gerrit reboot completed successfully. Downtimes removed.")
