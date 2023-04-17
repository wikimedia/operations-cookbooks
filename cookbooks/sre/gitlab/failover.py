"""GitLab failover cookbook"""

import time
import logging

from datetime import timedelta, date
from argparse import ArgumentParser

from spicerack.remote import RemoteHosts, RemoteExecutionError
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation, get_secret
from cookbooks.sre import CookbookBase, CookbookRunnerBase
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
    cookbook sre.gitlab.failover --current-primary gitlab1003 --new-primary gitlab2002 -t T12345
    """

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--current-primary",
            help="Current host that runs the primary gitlab instance",
        )
        parser.add_argument(
            "--new-primary",
            help="Host that we intend to be the new primary gitlab instance",
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

        self.current_primary = spicerack.remote().query(f"{args.current_primary}.*")
        self.new_primary = spicerack.remote().query(f"{args.new_primary}.*")
        self.alerting_hosts = self.spicerack.alerting_hosts(self.current_primary.hosts | self.new_primary.hosts)
        self.task_id = args.task
        self.downtime_id = None
        self.gitlab_token = get_secret("Gitlab API token")
        self.message = f"Failover of gitlab from {self.current_primary} to {self.new_primary}"

        self.reason = self.spicerack.admin_reason(reason=self.message, task_id=self.task_id)

        self.primary_gitlab_url = get_gitlab_url(self.current_primary)
        self.replica_gitlab_url = get_gitlab_url(self.new_primary)

        # Ensure the new host isn't already configured to be gitlab.wmo, I can't imagine a reason
        # to go this direction with the migration.
        if "gitlab.wikimedia.org" in self.replica_gitlab_url:
            raise RuntimeError(
                f"{self.new_primary} is already configured with gitlab.wikimedia.org. "
                "We probably never want to do this."
            )

        self.check_disk_space_available(self.current_primary)
        self.check_disk_space_available(self.new_primary)

        self.confirm_before_proceeding()

    def run(self) -> None:
        """Entrypoint to execute cookbook"""
        self.downtime_id = self.alerting_hosts.downtime(self.reason, duration=timedelta(hours=2))

        self.spicerack.puppet(self.current_primary).disable(self.reason)
        self.spicerack.puppet(self.new_primary).disable(self.reason)

        paused_runners = pause_runners(self.gitlab_token, self.primary_gitlab_url, dry_run=self.spicerack.dry_run)
        self.make_host_read_only(self.current_primary)

        backup_file = self.start_backup_on_old_host()
        self.transfer_backup_file(backup_file)

        # TODO: It would be nice to add in something that would check the host to make sure the role is applied
        # correctly before proceeding
        ask_confirmation(
            f"Please merge the change to set the puppet role for gitlab primary on {self.new_primary}. "
            "When you hit go, we will re-enable puppet and execute a puppet run"
        )
        self.spicerack.puppet(self.new_primary).run(enable_reason=self.reason)
        self.start_restore_process()

        # TODO: It would be nice to verify that these records are in place.
        ask_confirmation(
            f"Please merge a DNS update to point `{self.primary_gitlab_url}` to {self.new_primary} "
            f"and `{self.replica_gitlab_url}` to {self.current_primary}"
        )

        ask_confirmation(
            f"Please verify that the switchover to {self.primary_gitlab_url} is operating as expected. Once you are "
            f"certain please merge the change to set the puppet role for {self.current_primary}, and we will "
            " re-enable and run puppet."
        )
        self.spicerack.puppet(self.current_primary).run(enable_reason=self.reason)

        self.current_primary.run_sync("systemctl start ssh-gitlab", print_progress_bars=False)
        unpause_runners(paused_runners, dry_run=self.spicerack.dry_run)

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing"""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do and they can check the hosts are correct"""
        ask_confirmation(
            f"This will migrate {self.primary_gitlab_url} to {self.new_primary}, and "
            f"{self.replica_gitlab_url} to {self.current_primary}. Check that this is "
            "definitely what you want to do."
        )

    def check_disk_space_available(self, host: RemoteHosts) -> None:
        """Raises an exception if the disk space available is not sufficient"""
        if get_disk_usage_for_path(host, BACKUP_DIRECTORY) > DISK_HIGH_THRESHOLD:
            raise RuntimeError(f"Not enough disk space in {BACKUP_DIRECTORY}")

    def make_host_read_only(self, host) -> None:
        """Makes Gitlab on the current primary read-only

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

    def start_backup_on_old_host(self) -> str:
        """Starts a backup on the existing Gitlab host"""
        logger.info("Creates a backup on the old primary host.")
        logger.info("*** THIS IS SLOW. IT WILL TAKE 30-45 MINUTES ***")

        # Keep track of when we started the backup. Then look for filenames
        # newer than this to get the specific file to transfer to the new host
        time_backup_started = time.time()

        self.current_primary.run_sync(
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
            results = self.current_primary.run_sync(
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

        file = lines[0]
        # ls -t1 will list files in date order, newest first. We can assume the first file is newest
        first_file_timestamp = file.split("_")[0]
        # If we found a file, but it wasn't new enough, we might be in dry_run mode, since a new backup was never made
        if first_file_timestamp < int(backup_start_time) and not self.spicerack.dry_run:
            raise RuntimeError(
                f"Found {file}, but it is older than our backup start time {backup_start_time}"
            )

        return file

    def transfer_backup_file(self, backup_file: str) -> None:
        """Transfers backup file from old to new Gitlab host"""
        logger.info(
            "Starting to rsync %s to %s. This will take about 15 minutes",
            backup_file,
            self.new_primary,
        )

        self.current_primary.run_sync(
            f"/usr/bin/rsync -avp /srv/gitlab-backup/{backup_file} rsync://{self.new_primary}/data-backup",
            print_progress_bars=False
        )

    def start_restore_process(self) -> None:
        """Initiates the Gitlab restore process"""
        logger.info(
            "Starting restore process on %s. This will take about 20 minutes",
            self.new_primary,
        )

        self.new_primary.run_sync(f"{GITLAB_RESTORE_PATH}/gitlab-restore.sh -F", print_progress_bars=False)
