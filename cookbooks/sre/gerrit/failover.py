"""Gerrit failover cookbook.

This cookbook manages Gerrit failover operations between two hosts.
"""

import logging
import re
from datetime import timedelta
from argparse import ArgumentParser
from spicerack.decorators import retry
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation, ask_input
from wmflib.dns import Dns
from cookbooks.sre import CookbookBase, CookbookRunnerBase, PHABRICATOR_BOT_CONFIG_FILE

from . import GERRIT_DIR_PREFIX, GERRIT_BACKUP_PREFIX, GERRIT_DIRS

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
        return FailoverRunner(args, self.spicerack)

# Avoid raising Too many instance attributes (15/14)
# pylint: disable=too-many-instance-attributes


class FailoverRunner(CookbookRunnerBase):
    """Runner class for executing Failover."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 3600

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.dns = Dns()
        self.switch_from_host = spicerack.remote().query(f"{args.switch_from_host}.*")
        self.switch_to_host = spicerack.remote().query(f"{args.switch_to_host}.*")
        self.all_gerrit_hosts = spicerack.remote().query("gerrit*")
        self.message = f"from {self.switch_from_host} to {self.switch_to_host}"
        self.expected_src_address = self.dns.resolve_ipv4("gerrit.wikimedia.org")[0]
        msg = f"Retrieved source address: {self.expected_src_address}"
        logger.info(msg)
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.reason = self.spicerack.admin_reason(reason=self.message)
        self.args = args
        self.puppetserver = spicerack.puppet_server()

        # Hieradata lookups
        self.target_gerrit_user = self.puppetserver.hiera_lookup(
            self.switch_to_host.hosts[0],
            "profile::gerrit::daemon_user").splitlines()[-1]
        msg = f"Retrieved target gerrit user: {self.target_gerrit_user}"
        logger.info(msg)

        self.source_gerrit_site = self.puppetserver.hiera_lookup(
            self.switch_from_host.hosts[0],
            "profile::gerrit::gerrit_site").splitlines()[-1]
        msg = f"Retrieved source gerrit site: {self.source_gerrit_site}"
        if self.args.chown:
            self.target_gerrit_site = self.puppetserver.hiera_lookup(
                self.switch_to_host.hosts[0],
                "profile::gerrit::gerrit_site").splitlines()[-1]
        msg = f"Retrieved target gerrit site: {self.target_gerrit_site}"
        logger.info(msg)

        self.src_git_dir = self.puppetserver.hiera_lookup(
            self.switch_from_host.hosts[0],
            "profile::gerrit::git_dir").splitlines()[-1]
        msg = f"Retrieved target gerrit data dir for rsync exclusion: {self.src_git_dir}"
        logger.info(msg)
        self._pre_flight_check()
        self.confirm_before_proceeding()

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct."""
        ask_input(
            f"This will migrate gerrit.wikimedia.org from {self.switch_from_host} to {self.switch_to_host}. "
            f"Check that this is definitely what you want to do, by typing {self.switch_to_host}",
            choices=list(self.all_gerrit_hosts)
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
            args = [
                "--source", self.args.switch_from_host,
                "--replica", self.args.switch_to_host,
                "--full"
            ]
            if self.args.chown:
                args.append("--chown")
            self.spicerack.run_cookbook("sre.gerrit.topology-check", args=args, raises=True)

    def _run_cookbook_dns_cache_wipe(self) -> None:
        self.spicerack.run_cookbook("sre.dns.wipe-cache",
                                    args=[
                                        "gerrit.wikimedia.org",
                                        "gerrit-replica.wikimedia.org"
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
            "I will make the source instance read-only after you confirm that "
            f"{cmd} returns no more in progress replication. "
            "Please run that command and confirm that it is running so I can toggle the read-only mode."
        )
        #  replication source being frozen, we will now wait for replication

        self._run_cookbook_ro_toggle(host=self.switch_from_host.hosts[0].split('.')[0], state="on")

        ask_confirmation(
            "Please confirm replication is fully done."
        )

        # TODO offer a landing page either through Gerrit itself or through a http server
        self.switch_from_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False,
            print_output=True,
            is_safe=False
        )
        self.switch_to_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False,
            print_output=True,
            is_safe=False
        )
        # Freezing the target host to ensure consistency over restarts,
        # despite promotions/demotions
        self._run_cookbook_ro_toggle(host=self.switch_to_host.hosts[0].split('.')[0], state="on")
        #  TODO extract the local backup logic in another cookbook
        self._ensure_local_backup()
        if not self.args.distrust:
            self.sync_files(idempotent=True)
        else:
            self.confirm_before_proceeding()
            self.sync_files(idempotent=True, all_dirs=True)
        # After exchanging with Tyler on this, the conclusion was reached that Gerrit might leave
        # some replication behind during the replication process.
        # We should make sure nothing is left behind while switching hosts.

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
        self._run_cookbook_ro_toggle(host=self.switch_to_host.hosts[0].split('.')[0], state="off")
        self._run_cookbook_ro_toggle(host=self.switch_from_host.hosts[0].split('.')[0], state="off")
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
            "systemctl start gerrit",
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

    @retry(
        tries=10,
        delay=timedelta(seconds=3),
        backoff_mode="constant",
        failure_message="Waiting for rsync to be idempotent",
        exceptions=(RuntimeError,),
    )
    def sync_files(self, idempotent=False, all_dirs=False) -> bool:
        """Transfers files from old to new Gerrit host."""
        logger.info("Starting to rsync to %s.", self.switch_to_host)
        logger.warning("There will be a sync retry, expect no more than 10 of these.")
        command_sync_var_lib = (
            f"/usr/bin/rsync -avpPz --stats --delete {self.source_gerrit_site}/ "
            f" rsync://{self.switch_to_host}/gerrit-var-lib/"
        )
        if self.args.chown:
            command_sync_var_lib += " --no-o --no-g "
        if not all_dirs:
            command_sync_data = (
                "/usr/bin/rsync -avpPz --stats --delete /srv/gerrit/ "
                f"rsync://{self.switch_to_host}/gerrit-data/ "
                " --exclude=*.hprof "
                f" --exclude {self.src_git_dir} "
            )
        else:
            command_sync_data = (
                "/usr/bin/rsync -avpPz --stats --delete /srv/gerrit/ "
                f" rsync://{self.switch_to_host}/gerrit-data/ "
                " --exclude=*.hprof "
            )
        if self.args.chown:
            command_sync_data += " --no-o --no-g "

        if self.spicerack.dry_run:
            logger.info(
                "Would have run rsync commands %s and %s",
                command_sync_var_lib,
                command_sync_data
            )
        transfers = [
            self.switch_from_host.run_sync(
                command_sync_var_lib,
                print_progress_bars=False, print_output=False, is_safe=False
            ),
            self.switch_from_host.run_sync(
                command_sync_data,
                print_progress_bars=False, print_output=False, is_safe=False
            )
        ]
        if idempotent and not self.spicerack.dry_run:
            ret = [
                self._rsync_no_changes(list(t)[0][1].message().decode("utf-8")) for t in transfers
            ]
            if all(ret):
                if self.args.chown:
                    cmd = (f"chown -R {self.target_gerrit_user}:{self.target_gerrit_user} "
                           f"{GERRIT_DIR_PREFIX} {self.target_gerrit_site}")
                    logger.info("chowning files as --chown has been passed. Will use the following:")
                    logger.info(cmd)
                    self.switch_to_host.run_sync(
                        f"{cmd}",
                        print_progress_bars=False, print_output=False, is_safe=False
                    )
                return True
            raise RuntimeError("Rsync showed changes, retrying")
        return True

    def _rsync_no_changes(self, rsync_output: str) -> bool:
        """Check if rsync reports no changes in the transferred data."""
        patterns_expected_zero = {
            "Number of created files": 0,
            "Number of deleted files": 0,
            "Number of regular files transferred": 0,
        }

        for label, expected in patterns_expected_zero.items():
            pattern = rf"{re.escape(label)}:\s+(\d+)"
            match = re.search(pattern, rsync_output)
            if not match or int(match.group(1)) != expected:
                return False
        logger.info("Idempotency of rsync confirmed.")
        return True

    def _ensure_local_backup(self) -> bool:
        """Ensure backup on the source Gerrit server"""
        hosts = [self.switch_from_host]

        for host in hosts:
            msg = f"Preparing local emergency backup on {host}"
            logger.info(msg)

            self._backup_dirs_on_host(host)

        return True

    def _backup_dirs_on_host(self, host) -> None:
        for directory in GERRIT_DIRS:
            src = f"{GERRIT_DIR_PREFIX}{directory}/"
            dst = f"{GERRIT_BACKUP_PREFIX}{directory}/"

            # Ensure destination exists before rsync; mkdir is idempotent.
            host.run_sync(
                f"/bin/mkdir -p {dst}",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )

            rsync_cmd = (
                "/usr/bin/rsync -aP --checksum --stats --delete-before "
                f"{src} {dst}"
            )
            logger.info("Running backup rsync on %s: %s", host, rsync_cmd)
            host.run_sync(
                rsync_cmd,
                print_progress_bars=False,
                print_output=True,
                is_safe=False
            )
            if self.spicerack.dry_run:
                logger.info("Would have run backup rsync on %s: %s", host, rsync_cmd)
