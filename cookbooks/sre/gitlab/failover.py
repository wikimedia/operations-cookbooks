"""GitLab failover cookbook"""

import time
import logging

from datetime import timedelta, date
from argparse import ArgumentParser
from urllib.parse import urlparse

from spicerack.remote import RemoteHosts, RemoteExecutionError
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation, get_secret, confirm_on_failure
from cookbooks.sre import CookbookBase, CookbookRunnerBase, PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.gitlab import get_gitlab_url, get_disk_usage_for_path, pause_runners, unpause_runners

BACKUP_DIRECTORY = '/srv/gitlab-backup'
GITLAB_CTL = "/usr/bin/gitlab-ctl"
GITLAB_RESTORE_PATH = "/srv/gitlab-backup/"
DISK_HIGH_THRESHOLD = 40

logger = logging.getLogger(__name__)


class Failover(CookbookBase):
    """Performs a failover from one Gitlab host to another

    Instructions for failover are taken from these sources:
    https://wikitech.wikimedia.org/wiki/GitLab/Failover
    https://phabricator.wikimedia.org/T329931

    Prior to running this cookbook, you should also create the following patches
    (don't merge them until you've been prompted to do so though.):

    * Changing `profile::gitlab::service_name` to be `gitlab.wikimedia.org` on the host that will become
      the new primary, and `gitlab-replica.wikimedia.org` on the host that will become the replica.
      (Substitute this with `gitlab-replica` and `gitlab-replica-old`, or others if switching replicas)

      Example: https://gerrit.wikimedia.org/r/c/operations/puppet/+/891863


    * Changing the DNS A, AAAA, and PTR records to point the public recorsds to the correct hosts.

      Example: https://gerrit.wikimedia.org/r/c/operations/dns/+/891888


    Usage example:

    # Changes gitlab.wmo from gitlab1003 to gitlab2002, and gitlab-replica.wmo from gitlab2002 to gitlab1003
    cookbook sre.gitlab.failover --switch-from gitlab1003 --switch-to gitlab2002 -t T12345
    """

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--switch-from-host",
            help="Host that we want to switch away from (e.g., existing gitlab.wm.o, will become gitlab-replica.wm.o)",
        )
        parser.add_argument(
            "--switch-to-host",
            help="Host that we want to switch to (e.g., existing gitlab-replica.wm.o, will become gitlab.wm.o)",
        )
        parser.add_argument(
            "-t",
            "--task",
            required=False,
            help="Optional task ID to refer to in the downtime message",
        )

        return parser

    def get_runner(self, args):
        """Creates Spicerack runner"""
        return FailoverRunner(args, self.spicerack)


class FailoverRunner(CookbookRunnerBase):
    """Runner class for executing Failover"""

    def __init__(self, args, spicerack) -> None:
        """Initialize failover runner"""
        ensure_shell_is_durable()

        self.spicerack = spicerack

        self.switch_from_host = spicerack.remote().query(f"{args.switch_from_host}.*")
        self.switch_to_host = spicerack.remote().query(f"{args.switch_to_host}.*")
        self.gitlab_token = get_secret("Gitlab API token")
        self.message = f"Failover of gitlab from {self.switch_from_host} to {self.switch_to_host}"
        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
        else:
            self.phabricator = None

        self.reason = self.spicerack.admin_reason(reason=self.message, task_id=self.task_id)

        self.switch_from_gitlab_url = get_gitlab_url(self.switch_from_host)
        self.switch_to_gitlab_url = get_gitlab_url(self.switch_to_host)

        self.dns = self.spicerack.dns()
        self.pre_migration_ips = {
            self.switch_from_gitlab_url: sorted(self.dns.resolve_ips(urlparse(self.switch_from_gitlab_url).netloc)),
            self.switch_to_gitlab_url: sorted(self.dns.resolve_ips(urlparse(self.switch_to_gitlab_url).netloc)),
        }

        # Until we merge any changes to puppet or DNS, we can effectively roll back by unpausing runners
        # and restarting gitlab services. Keep track of whether we've merged changes so we can warn the user
        self.safe_rollback = True
        self.paused_runners = None

        # Ensure the new host isn't already configured to be gitlab.wmo, I can't imagine a reason
        # to go this direction with the migration.
        if "gitlab.wikimedia.org" in self.switch_to_gitlab_url:
            raise RuntimeError(
                f"{self.switch_to_host} is already configured with gitlab.wikimedia.org. "
                "We probably never want to do this."
            )

        self.check_disk_space_available(self.switch_from_host)
        self.check_disk_space_available(self.switch_to_host)

        self.confirm_before_proceeding()

    def run(self) -> None:
        """Entrypoint to execute cookbook"""
        self.maybe_task_comment(f'Cookbook {__name__} ({self.runtime_description}) started')

        alerting_hosts = self.spicerack.alerting_hosts(self.switch_from_host.hosts | self.switch_to_host.hosts)
        alerting_hosts.downtime(self.reason, duration=timedelta(hours=2))

        self.spicerack.puppet(self.switch_from_host).disable(self.reason)
        self.spicerack.puppet(self.switch_to_host).disable(self.reason)

        self.paused_runners = pause_runners(
            self.gitlab_token, self.switch_from_gitlab_url, dry_run=self.spicerack.dry_run
        )
        self.make_host_read_only(self.switch_from_host)

        backup_file = self.start_backup_on_switch_from_host()
        self.transfer_backup_file(backup_file)

        self.safe_rollback = False

        # TODO: It would be nice to add in something that would check the host to make sure the role is applied
        # correctly before proceeding
        ask_confirmation(
            f"Please merge the change to set the puppet role for gitlab primary on {self.switch_to_host}. "
            "When you hit go, we will re-enable puppet and execute a puppet run"
        )
        self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)
        self.start_restore_process()

        ask_confirmation(
            f"Please merge a DNS update to point `{self.switch_from_gitlab_url}` to {self.switch_to_host} "
            f"and `{self.switch_to_gitlab_url}` to {self.switch_from_host}"
        )
        confirm_on_failure(self.check_for_correct_dns)

        ask_confirmation(
            f"Please verify that the switchover to {self.switch_from_gitlab_url} is operating as expected. "
            f"Once you are certain please merge the change to set the puppet role for {self.switch_from_host}, "
            "and we will re-enable and run puppet."
        )
        self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)

        self.switch_from_host.run_sync("systemctl start ssh-gitlab", print_progress_bars=False)
        unpause_runners(self.paused_runners, dry_run=self.spicerack.dry_run)

        self.maybe_task_comment(f'Cookbook {__name__} ({self.runtime_description}) finished')

    def rollback(self) -> None:
        """Provides cleanup/rollback fixes if the cookbook is interrupted mid-execution"""
        self.maybe_task_comment(
            f'Cookbook {__name__} ({self.runtime_description}) encountered errors. Rollback started'
        )

        if not self.safe_rollback:
            # Using ask_confirmation because this is important, we need the user to see it.
            ask_confirmation(
                "We are rolling back the failover. Since you have merged puppet and/or DNS changes, it's *possible* "
                f"that {self.switch_from_host} and {self.switch_to_host} are no longer in sync. This needs to be "
                "manually addressed. Please read the 'Aborting Failover/Rollback' section below, and hit go. After "
                "that, puppet will be re-enabled and the runners unpaused automatically. "
                "https://wikitech.wikimedia.org/wiki/GitLab/Failover#Aborting_Failover/Rollback"
            )

        ask_confirmation(
            "We will now unpause runners, re-enable and run puppet, and restart gitlab services. Please ensure that "
            "either you have not merged any of the pre-prepared changes, or if you have that they have been reverted"
        )

        unpause_runners(self.paused_runners, dry_run=self.spicerack.dry_run)

        # Re-enable puppet on all hosts
        self.spicerack.puppet(self.switch_from_host).run(enable_reason=self.reason)
        self.spicerack.puppet(self.switch_to_host).run(enable_reason=self.reason)

        self.switch_from_host.run_sync("gitlab-ctl restart", progress_bars=False, is_safe=False)
        self.switch_from_host.run_sync("systemctl restart ssh-gitlab", progress_bars=False, is_safe=False)
        self.switch_from_host.run_sync("gitlab-ctl deploy-page down", progress_bars=False, is_safe=False)

        self.maybe_task_comment(
            f'Cookbook {__name__} ({self.runtime_description}) encountered errors. Rollback completed'
        )

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing"""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct"""
        ask_confirmation(
            f"This will migrate {self.switch_from_gitlab_url} to {self.switch_to_host}, and "
            f"{self.switch_to_gitlab_url} to {self.switch_from_host}. Check that this is "
            "definitely what you want to do."
        )

    def check_disk_space_available(self, host: RemoteHosts) -> None:
        """Raises an exception if the disk space available is not sufficient"""
        if get_disk_usage_for_path(host, BACKUP_DIRECTORY) > DISK_HIGH_THRESHOLD:
            raise RuntimeError(f"Not enough disk space in {BACKUP_DIRECTORY}")

    def make_host_read_only(self, host) -> None:
        """Makes Gitlab on the switch_from host read-only

        - Disabling sidekiq and Puma
        - Stop the ssh-gitlab service
        - Puts up the deploy page
        """
        logger.info("Disabling sidekiq and puma on %s", host)
        host.run_sync(f"{GITLAB_CTL} stop sidekiq && {GITLAB_CTL} graceful-kill puma", print_progress_bars=False)

        logger.info("Disabling ssh-gitlab on %s", host)
        host.run_sync("systemctl stop ssh-gitlab", print_progress_bars=False)

        logger.info("Placing 'deploy' page on %s", host)
        host.run_sync(f"{GITLAB_CTL} deploy-page up", print_progress_bars=False)

    def start_backup_on_switch_from_host(self) -> str:
        """Starts a backup on the existing Gitlab host"""
        logger.info("Creates a backup on the switch_from host.")
        logger.info("*** THIS IS SLOW. IT WILL TAKE 30-45 MINUTES ***")

        # Keep track of when we started the backup. Then look for filenames
        # newer than this to get the specific file to transfer to the new host
        time_backup_started = time.time()

        self.switch_from_host.run_sync(
            "/usr/bin/gitlab-backup create CRON=1 STRATEGY=copy "
            'GZIP_RSYNCABLE="true" GITLAB_BACKUP_MAX_CONCURRENCY="4" '
            'GITLAB_BACKUP_MAX_STORAGE_CONCURRENCY="2"',
            print_progress_bars=False
        )

        return self.find_backup_file(time_backup_started)

    def find_backup_file(self, backup_start_time: float) -> str:
        """Searches for the most recently created backup file"""
        today = date.today().strftime("%Y_%m_%d")
        file_pattern = f"*_{today}*_gitlab_backup.tar"

        try:
            results = self.switch_from_host.run_sync(
                f"ls -t1 {BACKUP_DIRECTORY}/{file_pattern}",
                print_progress_bars=False,
                is_safe=True
            )
        except RemoteExecutionError:
            logger.error("Couldn't list backup files, caught an exception")
            raise

        lines = []
        for _, output in results:
            lines = output.message().decode().split()

        file = lines[0].split("/")[-1]
        # ls -t1 will list files in date order, newest first. We can assume the first file is newest
        first_file_timestamp = file.split("_")[0]
        # If we found a file, but it wasn't new enough, we might be in dry_run mode, since a new backup was never made
        if int(first_file_timestamp) < int(backup_start_time) and not self.spicerack.dry_run:
            raise RuntimeError(
                f"Found {file}, but it is older than our backup start time {backup_start_time}"
            )

        return file

    def transfer_backup_file(self, backup_file: str) -> None:
        """Transfers backup file from old to new Gitlab host"""
        logger.info(
            "Starting to rsync %s to %s. This will take about 15 minutes",
            backup_file,
            self.switch_to_host,
        )

        self.switch_from_host.run_sync(
            f"/usr/bin/rsync -avp /srv/gitlab-backup/{backup_file} rsync://{self.switch_to_host}/data-backup",
            print_progress_bars=False
        )

    def start_restore_process(self) -> None:
        """Initiates the Gitlab restore process"""
        logger.info(
            "Starting restore process on %s. This will take about 20 minutes",
            self.switch_to_host,
        )

        self.switch_to_host.run_sync(f"{GITLAB_RESTORE_PATH}/gitlab-restore.sh -F", print_progress_bars=False)

    def check_for_correct_dns(self) -> None:
        """Raises an exception if the IP addresses haven't changed since before the migration started"""
        # The tool underlying the wipe-cache cookbook takes space-separated arguments, but run_cookbook needs a list
        self.spicerack.run_cookbook(
            'sre.dns.wipe-cache', [" ".join([self.switch_from_gitlab_url, self.switch_to_gitlab_url])]
        )

        switch_from_host_ips = sorted(self.dns.resolve_ips(urlparse(self.switch_from_gitlab_url).netloc))
        switch_to_host_ips = sorted(self.dns.resolve_ips(urlparse(self.switch_to_gitlab_url).netloc))

        if switch_from_host_ips != self.pre_migration_ips[self.switch_to_gitlab_url]:
            raise RuntimeError(
                f"IP for {self.switch_from_gitlab_url} doesn't match the pre-migration IPs for "
                f"{self.switch_to_gitlab_url}. Has the DNS change been merged? Or maybe it's cached somewhere. "
                f"(Should be {self.pre_migration_ips[self.switch_to_gitlab_url]}, but is {switch_from_host_ips})"
            )
        if switch_to_host_ips != self.pre_migration_ips[self.switch_from_gitlab_url]:
            raise RuntimeError(
                f"IP for {self.switch_to_gitlab_url} doesn't match the pre-migration IPs for "
                f"{self.switch_from_gitlab_url}. Has the DNS change been merged? Or maybe it's cached somewhere. "
                f"(Should be {self.pre_migration_ips[self.switch_from_gitlab_url]}, but is {switch_to_host_ips})"
            )

    def maybe_task_comment(self, message: str) -> None:
        """Comments on a phabricator task with a message, if the task ID is set and we can access phabricator"""
        if self.phabricator is not None:
            self.phabricator.task_comment(self.task_id, message)
