"""Gerrit failover cookbook.

This cookbook manages Gerrit failover operations between two hosts.
"""

import logging
import re
import subprocess
from datetime import timedelta
from argparse import ArgumentParser
from spicerack.decorators import retry
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation, ask_input
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

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.reason = self.spicerack.admin_reason(reason=self.message)
        self.args = args
        self.confirm_before_proceeding()

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct."""
        ask_input(
            f"This will migrate gerrit.wikimedia.org to {self.switch_to_host}. "
            f"Check that this is definitely what you want to do, by typing {self.switch_to_host}:\n$ ",
            choices=['gerrit1003', 'gerrit2002', 'gerrit2003']
            # TODO probe spicerack for host list instead of hardcoding it
        )

    def probe_dns_on_host(self, host, name: str) -> set[str]:
        """Execute 'dig +short name' on the given host and return the set of IP addresses it resolves to."""
        result = host.run_sync(
            f"dig +short {name}",
            print_progress_bars=False,
            print_output=False
        )
        raw_output = result[0][1].message().decode().strip().splitlines()
        return {line for line in raw_output if line}

    def _validate_dns(self):
        expected_ip = self.switch_to_host.hosts[0].ip
        self._ensure_dns_propagated("gerrit.wikimedia.org", expected_ip)

    def _run_cookbook_dns_cache_wipe(self):
        exit_code = self.spicerack.run_cookbook("sre.dns.wipe-cache",
                                                args=[
                                                    "gerrit.wikimedia.org",
                                                    "gerrit-replica.wikimedia.org"
                                                ], raises=True)
        if exit_code != 0:
            raise RuntimeError("Failed to wipe DNS cache")

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
                ips = self.probe_dns_on_host(srv, hostname)
            except subprocess.CalledProcessError as e:
                logger.warning("DNS probe error on %s: %s", srv, e)
                ips = set()
            seen[srv.hosts[0].name] = ips
            logger.debug("DNS seen by %s -> %s", srv.hosts[0].name, ips)

        if any(ips != {expected_ip} for ips in seen.values()):
            raise RuntimeError(f"DNS not stable yet: {seen}")

        logger.info("DNS propagated consistently to %s on both servers", expected_ip)

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        alerting_hosts = self.spicerack.alerting_hosts(
            self.switch_from_host.hosts | self.switch_to_host.hosts
        )
        alerting_hosts.downtime(self.reason, duration=timedelta(hours=4))
        self.spicerack.puppet(self.switch_to_host).disable(self.reason)
        self.spicerack.puppet(self.switch_from_host).disable(self.reason)
        ask_confirmation(
            "Run sudo -i authdns-update on ns0.wikimedia.org, review the diff but **do not commit yet.**"
        )

        # TODO offer a landing page either through Gerrit itself or through a http server
        self.switch_from_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False, print_output=True
        )
        self.switch_to_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False, print_output=True
        )
        self._ensure_backup_on_both_sides()
        exit_code = self.spicerack.run_cookbook("sre.gerrit.fsck",
                                                args=[
                                                    "--host",
                                                    f"{self.args.switch_from_host}.*"
                                                ], raises=True, confirm=True,)
        if exit_code != 0:
            raise RuntimeError("Failed to run FSCK on Gerrit backup tree.")
        self.sync_files(idempotent=True)

        ask_confirmation(
            f"Please merge the change to set the DNS records for Gerrit primary on {self.switch_to_host}. "
            "I will wait for propagation across both Gerrit hosts."
        )
        self._post_sync_dst_validate()
        self._post_sync_src_validate()

    def _post_sync_dst_validate(self):
        if not self.spicerack.dry_run:
            self._run_cookbook_dns_cache_wipe()
            self._validate_dns()

        ask_confirmation(
            f"Please merge the change to set the puppet role for Gerrit primary on {self.switch_to_host}. "
            "When you hit go, we will re-enable puppet and execute a puppet run."
        )
        self.confirm_before_proceeding()
        self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)
        if self._grep_for_replica(self.switch_to_host):
            raise RuntimeError("Failed configuration on destination host, found replica flag still enabled.")
        self.switch_to_host.run_sync(
            "systemctl restart gerrit",
            print_progress_bars=False, print_output=True
        )
        # TODO https://gerrit-review.googlesource.com/Documentation/rest-api-config.html#check-consistency on the source
        ask_confirmation(
            "Please verify that the switchover to gerrit.wikimedia.org is operating as expected. "
            f"Once you are certain please merge the change to set the puppet role for {self.switch_from_host}, "
            "and we will re-enable and run puppet."
        )
        return True

    def _post_sync_src_validate(self):
        if not self.spicerack.dry_run:
            self._validate_dns()
        self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)
        self.switch_from_host.run_sync(
            "systemctl stop gerrit",
            print_progress_bars=False, print_output=True
        )
        if self._grep_for_replica(self.switch_to_host):
            msg = (
                "Failed configuration on destination host, found replica missing."
                "This is a split brain situation."
            )
            raise RuntimeError(msg)
        self.switch_from_host.run_sync(
            "systemctl start gerrit",
            print_progress_bars=False, print_output=True
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
                self._rsync_no_changes(list(t)[0][1].message().decode("utf-8")) for t in transfers
            ]
            if all(ret):
                return True
            raise RuntimeError("Rsync showed changes, retrying")

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
            pattern = rf"{re.escape(label)}:\\s+(\\d+)"
            match = re.search(pattern, rsync_output)
            if not match or int(match.group(1)) != expected:
                return False

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
            )

    def _grep_for_replica(self, host) -> bool:
        grep_cmd = (
            "/bin/grep -q replica "
            "/etc/systemd/system/multi-user.target.wants/gerrit.service "
            "&& echo FOUND || echo NOTFOUND"
        )

        result = host.run_sync(
            grep_cmd,
            print_progress_bars=False,
            print_output=False,
        )

        output = result[0][1].message().decode().strip()
        return output == "FOUND"
