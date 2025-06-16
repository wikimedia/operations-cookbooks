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
            " This means that the directory containing git data will **NOT**"
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
            "This argument needs to be used with caution."
            " It will run git fsck on all backed up repos."
            " It take a long time to complete."
        )
        parser.add_argument(
            "--fsck",
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
        self.gerrit_host_list = list(spicerack.remote().query("gerrit*"))
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
        self.source_gerrit_user = self.puppetserver.hiera_lookup(
            self.switch_from_host.hosts[0],
            "profile::gerrit::daemon_user").splitlines()[-1]
        msg = f"Retrieved target gerrit user: {self.target_gerrit_user}"
        logger.info(msg)
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
            f"This will migrate gerrit.wikimedia.org to {self.switch_to_host}. "
            f"Check that this is definitely what you want to do, by typing {self.switch_to_host}",
            choices=self.gerrit_host_list
        )

    def _pre_flight_check(self) -> None:
        logger.info("Running pre-flight checks")
        if not self._grep_for_replica(host=self.switch_to_host):
            raise RuntimeError("target host is not configured as replica.")
        if self.expected_src_address != self._dig_on_host(self.switch_to_host, 'gerrit.wikimedia.org'):
            raise RuntimeError("Unexpected IP after DNS check.")
        if not self._is_gerrit_running(self.switch_to_host):
            raise RuntimeError("Gerrit is in an unexpected state.")
        if not self._gerrit_user_check():
            if not self.args.chown:
                raise RuntimeError("Gerrit is in an unexpected state.")
            logger.info("User discrepancy has been handled via --chown, continuing.")

    def _gerrit_user_check(self) -> bool:
        return self.target_gerrit_user == self.source_gerrit_user

    def _post_flight_check(self, before_puppet_on_future_replica=True) -> None:
        logger.info("Running post-flight checks")
        if before_puppet_on_future_replica:
            # We want to skip that check since puppet has not yet been able to change that configuration
            if not self._grep_for_replica(host=self.switch_from_host):
                if not self.spicerack.dry_run:
                    raise RuntimeError("Source host not configured as replica, please advise.")
                logger.info("Source host not configured as replica, as expected, due to dry-run.")
        if not self._dig_on_host(self.switch_to_host, 'gerrit.wikimedia.org'):
            if not self.spicerack.dry_run:
                raise RuntimeError("Unexpected IP after DNS check.")
            logger.info("Unexpected IP after DNS check, as expected, due to dry-run.")
        if self._dig_on_host(self.switch_from_host, 'gerrit.wikimedia.org'):
            err = f"{self.switch_from_host.hosts[0]} should not be identified as gerrit.wikimedia.org in DNS."
            if not self.spicerack.dry_run:
                raise RuntimeError(err)
            err += ", as expected due to dry-run"
            logger.info(err)

    def _is_gerrit_running(self, host) -> bool:
        result = list(host.run_sync(
            "systemctl is-running gerrit.service",
            is_safe=True,
            print_progress_bars=False,
            print_output=False,
        ))
        output = result[0][1].message().decode().strip()
        if output != "inactive":
            logger.info("gerrit is running on this host")
        else:
            logger.info("gerrit is stopped on this host")
        return output != "inactive"

    def _validate_dns(self) -> None:
        expected_ip = self.dns.resolve_ipv4("gerrit.wikimedia.org")[0]
        self._ensure_dns_propagated("gerrit.wikimedia.org", expected_ip)

    @retry(
        tries=3,
        delay=timedelta(seconds=15),
        backoff_mode="constant",
        failure_message="Failed to properly run cache wipe cookbook.",
        exceptions=(RuntimeError,),
    )
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
                                            f"--host {host}",
                                            f"--toggle {state}"
                                        ], raises=True)
        else:
            ask_confirmation(
                "This cookbook will run without read-only state being activated on instances."
            )

    @retry(
        tries=40,
        delay=timedelta(seconds=15),
        backoff_mode="constant",
        failure_message="Waiting for DNS propagation failed",
        exceptions=(RuntimeError,),
    )
    def _ensure_dns_propagated(self, hostname: str, expected_ip: str) -> None:
        """Probe both Gerrit servers and assert they both resolve 'hostname' to exactly {expected_ip}.

        Raises:
            RuntimeError: triggers retry if DNS is not yet stable.

        """
        servers = [self.switch_from_host, self.switch_to_host]
        seen = {}

        for srv in servers:
            try:
                ips = set(self._dig_on_host(srv, hostname))
            except RuntimeError as e:
                logger.warning("DNS probe error on %s: %s", srv, e)
                ips = set()
            seen[srv.hosts[0].name] = ips
            logger.debug("DNS seen by %s -> %s", srv.hosts[0].name, ips)

        if any(ips != {expected_ip} for ips in seen.values()):
            raise RuntimeError(f"DNS not stable yet: {seen}")

        logger.info("DNS propagated consistently to %s on both servers", expected_ip)

    def _dig_on_host(self, host, name: str):
        """Execute 'dig +short name' on the given host."""
        result = list(host.run_sync(
            f"dig +short {name}",
            print_progress_bars=False,
            print_output=False,
            is_safe=True,
            raises=True
        ))
        result = result[0][1].message().decode().strip()
        msg = f"{result} was found for this dig query."
        logger.info(msg)
        return result

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alerting_hosts = self.spicerack.alerting_hosts(
            self.switch_from_host.hosts | self.switch_to_host.hosts
        )
        if not self.spicerack.dry_run:
            alerting_hosts.downtime(self.reason, duration=timedelta(hours=4))
            self.spicerack.puppet(self.switch_to_host).disable(self.reason)
            self.spicerack.puppet(self.switch_from_host).disable(self.reason)
        ask_confirmation(
            "Run sudo -i authdns-update on ns0.wikimedia.org, review the diff but **do not commit yet.**. "
            "You will be asked later on to commit."
        )
        cmd = "watch ssh gerrit.wikimedia.org -p 29418 gerrit show-queue --by-queue --wide"
        ask_confirmation(
            "I will make the source instance read-only after you confirm that "
            f"{cmd} returns no more pending replication thread. "
            "Please run that command and confirm that it is running so I can toggle the read-only mode."
        )
        self._run_cookbook_ro_toggle(host=self.switch_from_host.hosts[0], state="on")
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
        self._run_cookbook_ro_toggle(host=self.switch_to_host.hosts[0], state="on")
        self._ensure_backup_on_both_sides()
        if self.args.fsck:
            self.spicerack.run_cookbook("sre.gerrit.fsck",
                                        args=[
                                            "--host",
                                            f"{self.args.switch_from_host}.*"
                                        ], raises=True, confirm=True,)

        self.sync_files(idempotent=True)
        if self.args.distrust:
            self.confirm_before_proceeding()
            self.sync_files(idempotent=True, all_dirs=True)
        # After exchanging with Tyler on this, the conclusion was reached that Gerrit might leave
        # some replication behind during the replication process.
        # We should make sure nothing is left behind while switching hosts.

        ask_confirmation(
            f"Please merge the change to set the DNS records for Gerrit primary on {self.switch_to_host}. "
            "I will wait for propagation across both Gerrit hosts."
        )
        if not self.spicerack.dry_run:
            self._run_cookbook_dns_cache_wipe()
        self._post_sync_dst_validate()
        self._post_flight_check()
        self._post_sync_src_validate()
        ask_confirmation(
            "This is a danger zone, we will now enable writes on both instances. "
            "Please make sure the cluster status is nominal before going further."
        )
        self._run_cookbook_ro_toggle(host=self.switch_to_host.hosts[0], state="off")
        self._run_cookbook_ro_toggle(host=self.switch_from_host.hosts[0], state="off")

    def _post_sync_dst_validate(self) -> None:
        if not self.spicerack.dry_run:
            self._validate_dns()

        ask_confirmation(
            f"Please merge the change to set the puppet role for Gerrit primary on {self.switch_to_host}. "
            "When you hit go, we will re-enable puppet and execute a puppet run."
        )
        self.confirm_before_proceeding()
        if not self.spicerack.dry_run:
            self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)
        if self._grep_for_replica(self.switch_to_host) and not self.spicerack.dry_run:
            raise RuntimeError("Failed configuration on destination host, found replica flag still enabled.")
        self.switch_to_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True, is_safe=False
        )
        # TODO https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#check-consistency on the source
        ask_confirmation(
            "Please verify that the switchover to gerrit.wikimedia.org is operating as expected. "
            f"Once you are certain please merge the change to set the puppet role for {self.switch_from_host}, "
            "and we will re-enable and run puppet."
        )

    def _post_sync_src_validate(self) -> None:
        if not self.spicerack.dry_run:
            self._validate_dns()
            self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)
            self.switch_from_host.run_sync(
                "systemctl stop gerrit",
                print_progress_bars=False, print_output=True, is_safe=False
            )
            self._post_flight_check(before_puppet_on_future_replica=False)

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
        command_sync_var_lib = (
            f"/usr/bin/rsync -avpPz --stats --delete {self.target_gerrit_site} "
            f" rsync://{self.switch_to_host}/gerrit-var-lib/"
        )

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
                return True
            raise RuntimeError("Rsync showed changes, retrying")

        if self.args.chown:
            self.switch_to_host.run_sync(
                f"chown -R {self.target_gerrit_user}:{self.target_gerrit_user} {GERRIT_DIR_PREFIX}",
                print_progress_bars=False, print_output=False, is_safe=False
            )

        return True

    def _rsync_no_changes(self, rsync_output: str) -> bool:
        #  TODO revalidate idempotency
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

    def _ensure_backup_on_both_sides(self) -> bool:
        """Ensure backup on *both* Gerrit servers"""
        hosts = [self.switch_from_host, self.switch_to_host]

        for host in hosts:
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
                "/usr/bin/rsync -aP --checksum --stats --delete "
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

    def _grep_for_replica(self, host) -> bool:
        grep_cmd = (
            "/bin/grep -q replica "
            "/etc/systemd/system/multi-user.target.wants/gerrit.service "
            "&& echo FOUND || echo MISSING"
        )

        result = list(host.run_sync(
            grep_cmd,
            print_progress_bars=False,
            print_output=False,
            is_safe=True
        ))
        result = result[0][1].message().decode().strip()
        if result == "FOUND":
            logger.info("Found that this host is configured as a replica.")
        else:
            logger.info("Found that this host is configured as a source.")
        return result == "FOUND"
