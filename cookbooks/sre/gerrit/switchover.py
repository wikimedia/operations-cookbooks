"""Gerrit switchover cookbook.

This cookbook manages Gerrit switchover operations between two hosts.
"""

import logging
from argparse import ArgumentParser
from datetime import timedelta

from wmflib.interactive import ask_confirmation, ask_input, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE, CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)


class Switchover(CookbookBase):
    """Performs a switchover from one Gerrit host to another."""

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
        warning = (
            "This argument needs to be used with caution."
            " We will distrust gerrit's replication to run rsync."
            " This means that the directory containing git data will"
            " be replicated via rsync through this cookbook."
        )
        parser.add_argument(
            "--distrust",
            required=False,
            default=False,
            action='store_true',
            help=warning,
        )
        warning = (
            "This argument is design to handle user"
            " migration between gerrit instances."
        )
        parser.add_argument(
            "--chown",
            required=False,
            default=False,
            action='store_true',
            help=warning,
        )
        warning = (
            "This argument will **skip**"
            " Gerrit's read-only plugin management."
        )
        parser.add_argument(
            "--rw",
            required=False,
            default=False,
            action='store_true',
            help=warning,
        )

        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return SwitchoverRunner(args, self.spicerack)

# Avoid raising Too many instance attributes (15/14)
# pylint: disable=too-many-instance-attributes


class SwitchoverRunner(CookbookRunnerBase):
    """Runner class for executing Switchover."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 3600

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.switch_from_host = spicerack.remote().query(f"{args.switch_from_host}.*")
        self.switch_to_host = spicerack.remote().query(f"{args.switch_to_host}.*")
        self.all_gerrit_hosts = spicerack.remote().query("gerrit*")
        self.message = f"from {self.switch_from_host} to {self.switch_to_host}"
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.reason = self.spicerack.admin_reason(reason=self.message)
        self.args = args
        self._pre_flight_check()
        self.confirm_before_proceeding()

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct."""
        ask_input(
            f"This will migrate gerrit.wikimedia.org from {self.args.switch_from_host} to {self.args.switch_to_host}. "
            f"Check that this is definitely what you want to do, by typing {self.args.switch_to_host}",
            choices=[self.args.switch_to_host],
        )

    def _pre_flight_check(self) -> None:
        args = [
            "--source", self.args.switch_from_host,
            "--replica", self.args.switch_to_host,
            "--full"
        ]
        if self.args.chown:
            args.append("--chown")
        self.spicerack.run_cookbook("sre.gerrit.topology-check", args=args, raises=True)

    def _post_sync_validate_new_source(self) -> None:
        logger.info("Running post-flight checks for the new source")
        if not self.spicerack.dry_run:
            args = [
                "--source", self.args.switch_to_host,
                "--replica", self.args.switch_from_host,
                "--full"
            ]
            if self.args.chown:
                args.append("--chown")
            self.spicerack.run_cookbook("sre.gerrit.topology-check", args=args, raises=True)
        else:
            # Using dry-run implies the topology will not change
            #  we'll use the same source/replica as in pre-flight
            #  to avoid throwing an error.
            logger.info("Running post-flight checks for the old source, dry-run mode is enabled")
            args = [
                "--source", self.args.switch_from_host,
                "--replica", self.args.switch_to_host,
                "--full"
            ]
            if self.args.chown:
                args.append("--chown")
            self.spicerack.run_cookbook("sre.gerrit.topology-check", args=args, raises=True)

    def _run_cookbook_localbackup(self, source) -> None:
        self.spicerack.run_cookbook("sre.gerrit.localbackup",
                                    args=[
                                        "--source", source,
                                    ], raises=True)

    def _run_cookbook_sync_instances(self, args) -> None:
        logger.info("Will run sync-instances cookbook with: %s", args)
        self.spicerack.run_cookbook("sre.gerrit.sync-instances",
                                    args=args, raises=True)

    def _run_cookbook_dns_cache_wipe(self) -> None:
        self.spicerack.run_cookbook("sre.dns.wipe-cache",
                                    args=[
                                        "gerrit.wikimedia.org",
                                        "gerrit-replica.wikimedia.org",
                                        "gerrit.discovery.wmnet"
                                    ], raises=True)

    def _run_cookbook_ro_toggle(self, host, state) -> None:
        if not self.args.rw:
            self.spicerack.run_cookbook("sre.gerrit.read-only-toggle",
                                        args=[
                                            "--host", host,
                                            "--toggle", state
                                        ], raises=True)
        else:
            ask_confirmation(
                "This cookbook will run without read-only state being activated on instances."
            )

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alerting_hosts = self.spicerack.alerting_hosts(
            self.switch_from_host.hosts | self.switch_to_host.hosts
        )
        #  Skipped by dry-run/test-cookbook
        alerting_hosts.downtime(self.reason, duration=timedelta(hours=4))
        self.spicerack.puppet(self.all_gerrit_hosts).disable(self.reason)
        #  disabling puppet across all instances so we control the critical steps.
        ask_confirmation(
            "Run sudo -i authdns-update on ns0.wikimedia.org, review the diff but **do not commit yet.**. "
            "You will be asked later on to commit."
        )
        ask_confirmation(
            f"Please merge the change to set the **puppet role** for Gerrit primary on {self.switch_to_host}. "
        )
        cmd = "sudo tail -fn0 /var/log/gerrit/replication_log"
        ask_confirmation(
            "Please run "
            f"{cmd} and confirm here when its done. "
            "The next step will toggle ON read-only mode "
        )
        #  replication source being frozen, we will now wait for replication
        self._run_cookbook_ro_toggle(host=self.args.switch_from_host, state="on")
        ask_confirmation(
            "Please confirm replication is fully done."
        )
        self.switch_to_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False,
            print_output=True,
            is_safe=False
        )
        # Freezing the target host to ensure consistency over restarts,
        # despite promotions/demotions
        self._run_cookbook_ro_toggle(host=self.args.switch_to_host, state="on")
        self._run_cookbook_localbackup(source=self.args.switch_from_host)
        self.confirm_before_proceeding()

        sync_args = ["--source", self.args.switch_from_host,
                     "--replica", self.args.switch_to_host,
                     "--verbose",]

        if self.args.distrust:
            sync_args.append("--distrust")
        if self.args.chown:
            sync_args.append("--chown")

        self._run_cookbook_sync_instances(sync_args)

        ask_confirmation(
            f"Please merge the change to set the **DNS records** for Gerrit primary on {self.switch_to_host}. "
            "I will trigger then a DNS cache wipe and ensure both Gerrit hosts are up to speed."
        )

        if not self.spicerack.dry_run:
            self._run_cookbook_dns_cache_wipe()
        #  Validates that all instances are seeing the same resource record version.
        self._ensure_dns_post_merge()
        ask_confirmation(
            "When you hit go, we will re-enable puppet and execute a puppet run."
        )

        self.confirm_before_proceeding()
        self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)
        self._post_sync_validations()
        # Now that puppet has run on the new primary, we'll run the post-sync validations on the new replica
        # this method will first enable puppet and run a topology check to ensure everything is up to snuff.

        self.switch_to_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True, is_safe=False
        )

        #####
        # TODO https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#check-consistency on the source
        ##
        ask_confirmation(
            "Please verify that the switchover to gerrit.wikimedia.org is operating as expected. "
            "Once this is done, we will re-enable read-write on all instances after your confirmation."
        )
        self._run_cookbook_ro_toggle(host=self.args.switch_to_host, state="off")
        self._run_cookbook_ro_toggle(host=self.args.switch_from_host, state="off")
        logger.info("Running puppet-agent one last time across all Gerrit instances.")
        self.spicerack.puppet(self.all_gerrit_hosts).run(enable_reason=self.reason)

    def _post_sync_validations(self) -> None:
        self._post_sync_validate_former_source()
        self._post_sync_validate_new_source()

    def _post_sync_validate_former_source(self) -> None:
        #  this specific step is designed to ensure we avoid running into incidents such as https://w.wiki/EcxD
        logger.info("Running post-flight checks for the new replica")
        if not self.spicerack.dry_run:
            # DNS has been checked before this, we will enable puppet to have the --replica flag on
            self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)
            # if so, it is OK to enable puppet and have that --replica flag, and double check it
            self.spicerack.run_cookbook("sre.gerrit.topology-check",
                                        args=[
                                            "--host", self.args.switch_from_host,
                                            "--systemd",
                                        ], raises=True)

        self.switch_from_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True, is_safe=False
        )
        ask_confirmation(
            "Please verify that the switchover to gerrit-replica.wikimedia.org is also operating as expected, "
            "it should return a 404 on /. If needed, please follow the remaining guidelines listed here: "
            "https://w.wiki/DoeG"
        )
        # TODO https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#check-consistency on the repl

    def _ensure_dns_post_merge(self):
        self.spicerack.run_cookbook("sre.gerrit.topology-check",
                                    args=[
                                        # the new source should be identified as such
                                        "--source", self.args.switch_to_host,
                                        "--replica", self.args.switch_from_host,
                                        "--dns"
                                    ], raises=True)
