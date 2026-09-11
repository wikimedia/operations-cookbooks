"""GitLab version upgrade cookbook"""

import logging
from datetime import timedelta
from functools import cached_property
import re
import time
from packaging import version

import gitlab
from wmflib.interactive import ask_confirmation, ask_input, ensure_shell_is_durable, get_secret
from spicerack.alertmanager import AlertmanagerError
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.gitlab import (
    get_gitlab_url, get_disk_usage_for_path, lock_backups_on_host,
    pause_runners, unlock_backups_on_host, unpause_runners
)


BACKUP_DIRECTORY = "/srv/gitlab-backup"
BACKUP_LOCK_FILE = "/opt/gitlab/embedded/service/gitlab-rails/tmp/backup_restore.pid"
DISK_HIGH_THRESHOLD = 70
DOWNTIME_DURATION = 200  # in minutes
BACKUP_RESTORE_ALERTNAME = "SystemdUnitFailed"
BACKUP_RESTORE_SERVICE = "gitlab-backup-restore.service"
RESTORE_STALENESS_ALERTNAMES = "GitLabRestoreStale|GitLabReplicaDataStale|GitLabRestoreVersionMismatch"
BACKUP_RESTORE_DOWNTIME_DURATION = 60 # in hours
ATS_BACKEND_ALERTNAME = "ATSBackendErrorsHigh"
ATS_BACKEND = "gitlab.discovery.wmnet"
# ATSBackendErrorsHigh needs 15 minutes of sustained errors on top of a 5 minutes rate window,
# so its silence has to outlive the upgrade window instead of ending with it.
ATS_DOWNTIME_DURATION = DOWNTIME_DURATION + 30  # in minutes
# Hiera key holding the FQDN of the active (primary) GitLab host, any other host is a replica
ACTIVE_HOST_HIERA_KEY = "profile::gitlab::active_host"

logger = logging.getLogger(__name__)


class Upgrade(CookbookBase):
    """Upgrade GitLab hosts to a new version

    - Check disk space
    - Create full data backup
    - Create config backup
    - Fetch new Debian gitlab-ce package (download-only)
    - Pause Runners
    - Check for remaining background migrations
    - Downtime host
    - Mask services which require GitLab
    - Install new Debian gitlab-ce package
    - Wait for GitLab and Unpause Runners
    - Unmask services

    Usage example:
        cookbook sre.gitlab.upgrade --host gitlab1004 --version 15.4.4-ce.0 -r 'some reason' -t T12345

    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--host', required=True, help='Short hostname of the gitlab host to upgrade, not FQDN')
        parser.add_argument('--version', required=True,
                            help='Version of new GitLab Debian package in Debian versioning schema')
        parser.add_argument('-s', '--skip-replica-backups', action='store_true',
                            help='Skip creating a backup on replica hosts without asking')
        parser.add_argument("-c", "--skip-confirm-prompt", action='store_true',
                            help="Skip confirmation prompts before restarting hosts")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UpgradeRunner(args, self.spicerack)


class UpgradeRunner(CookbookRunnerBase):
    """Upgrade a GitLab host to a new version."""

    # pylint: disable=too-many-instance-attributes
    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.spicerack = spicerack
        self.host = args.host
        self.remote_host = spicerack.remote().query(f'{args.host}.*')
        self.alertmanager = self.spicerack.alertmanager()
        if len(self.remote_host) != 1:
            raise RuntimeError(f"Found the following hosts: {self.remote_host} for query {args.host}."
                               "Query must return 1 host.")
        self.fqdn = self.remote_host.hosts[0]
        self.url = get_gitlab_url(self.remote_host)

        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.service_alerting_hosts = spicerack.alertmanager_hosts(
            [re.sub(r'^https://|/$', '', self.url)], verbatim_hosts=True)

        self.task_id = args.task_id
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.target_version = args.version
        self.skip_confirm_prompt = args.skip_confirm_prompt

        # Skipping backups on the active host should not be possible
        if args.skip_replica_backups and not self.is_replica:
            raise RuntimeError(f"--skip-replica-backups can't be used on the active host {self.fqdn}")

        self.skip_replica_backups = args.skip_replica_backups
        # A replica's data comes from the active host's backup, so a local one is mostly
        # redundant: offer to skip it, without requiring -s. --skip-confirm-prompt is
        # deliberately not honoured here, skipping a backup is a decision of its own and
        # not a confirmation before a restart. Pass -s to run unattended on a replica.
        if not self.skip_replica_backups and self.is_replica:
            self.skip_replica_backups = ask_input(
                f"{self.fqdn} is a replica, OK to skip the local backup? "
                "Creating one takes about 15 minutes.", ["skip", "backup"]) == "skip"

        self.token = get_secret('GitLab API Token')
        self.gitlab_instance = gitlab.Gitlab(self.url, private_token=self.token)

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        self.check_gitlab_version()
        self.fail_for_disk_space()

        self.message = f'on GitLab host {self.remote_host} with reason: {args.reason}'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return self.message

    @property
    def lock_args(self):
        """Make the cookbook lock exclusive per-host."""
        return LockArgs(suffix=self.host, concurrency=1, ttl=7200)

    def rollback(self):
        """Comment on phabricator in case of a failed run."""
        self.phabricator.task_comment(
            self.task_id,
            f"Cookbook {__name__} started by {self.admin_reason.owner} executed with errors:\n"
            f"{self.runtime_description}\n"
        )
        logger.info("Applying apt hold on gitlab-ce package")
        self.remote_host.run_sync("apt-mark hold gitlab-ce")

    def run(self):
        """Run the cookbook."""
        self.phabricator.task_comment(
            self.task_id,
            f'Cookbook {__name__} was started by {self.admin_reason.owner} {self.runtime_description}')
        try:
            broadcastmessage = self.gitlab_instance.broadcastmessages.create({
                'message': f'Maintenance {self.message} starting soon.',
                'broadcast_type': 'notification'
            })
        except gitlab.exceptions.GitlabCreateError as e:
            raise RuntimeError("Unable to create broadcast message."
                               "Make sure your access token uses scope api and admin_mode.") from e

        logger.info("Releasing apt hold on gitlab-ce package")
        self.remote_host.run_sync("apt-mark unhold gitlab-ce")

        self.preload_debian_package()
        if self.skip_replica_backups:
            logger.info("Skipping creation of backups")
        else:
            self.create_data_backup()
            self.create_config_backup()

        self.fail_for_background_migrations()
        self.fail_for_running_backup()

        # Lock all backups and restores to ensure they do not interrupt the upgrade
        lock_backups_on_host(self.remote_host, BACKUP_DIRECTORY)

        if not self.skip_replica_backups and not self.skip_confirm_prompt:
            self.spicerack.irc_logger.info(
                f"{self.spicerack.username}: The backup on {self.host} is complete, ready to proceed with upgrade."
            )
            ask_confirmation(
                "The backup is complete, and we are ready to install the package. Gitlab will restart and be "
                "unavailable once you continue. Ready to go?"
            )

        # silence backup-restore.service and the gitlab restore staleness
        # alerts (until next restore happened)
        if self.is_replica:
            silences = [
                [
                    {"name": "alertname",
                        "value": BACKUP_RESTORE_ALERTNAME, "isRegex": False},
                    {"name": "name", "value": BACKUP_RESTORE_SERVICE,
                        "isRegex": False},
                ],
                [
                    {"name": "alertname",
                        "value": RESTORE_STALENESS_ALERTNAMES, "isRegex": True},
                    {"name": "host", "value": self.host.split(".")[0],
                        "isRegex": False},
                ],
            ]
            for matchers in silences:
                try:
                    self.alertmanager.downtime(
                        reason=self.admin_reason, matchers=matchers,
                        duration=timedelta(hours=BACKUP_RESTORE_DOWNTIME_DURATION))
                except AlertmanagerError as error:
                    logger.warning(
                        'Failed to create a silence for %s on %s: %s',
                        matchers,
                        self.host,
                        error,
                    )

        if not self.is_replica:
            try:
                matchers = [
                    {"name": "alertname", "value": ATS_BACKEND_ALERTNAME, "isRegex": False},
                    {"name": "backend", "value": ATS_BACKEND, "isRegex": False},
                ]
                self.alertmanager.downtime(
                    reason=self.admin_reason, matchers=matchers,
                    duration=timedelta(minutes=ATS_DOWNTIME_DURATION))
            except AlertmanagerError as error:
                logger.warning('Failed to create a silence for ATS backend %s: %s', ATS_BACKEND, error)

        paused_runners = pause_runners(self.token, self.url, dry_run=self.spicerack.dry_run)
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=DOWNTIME_DURATION)):
            # Also create a downtime for the service name (like gitlab.wikimedia.org)
            with self.service_alerting_hosts.downtimed(self.admin_reason,
                                                       duration=timedelta(minutes=DOWNTIME_DURATION)):
                self.remote_host.run_sync("systemctl mask sync-gitlab-group-with-ldap.service")
                self.install_debian_package()
                unpause_runners(paused_runners, dry_run=self.spicerack.dry_run)
                broadcastmessage.delete()
                logger.info('Wait for blackbox checks and monitoring to catch up.')
                time.sleep(180)
                self.remote_host.run_sync("systemctl unmask sync-gitlab-group-with-ldap.service")
                logger.info("Applying apt hold on gitlab-ce package")
                self.remote_host.run_sync("apt-mark hold gitlab-ce")

        self.phabricator.task_comment(
            self.task_id,
            f'Cookbook {__name__} started by {self.admin_reason.owner} {self.runtime_description} completed '
            f'successfully {self.runtime_description}')

        # Unlock backup and restore again
        unlock_backups_on_host(self.remote_host, BACKUP_DIRECTORY)

    def check_gitlab_version(self):
        """Compare current GitLab version with target version.

        Also prevent downgrade and ask confirmation for major upgrades.

        """
        logger.info('Get GitLab version from API')

        gitlab_version = self.gitlab_instance.version()[0]
        if gitlab_version == "unknown":
            raise RuntimeError("Failed to get GitLab version from API."
                               "Check instance, API token (scope api and admin_mode) and URL")

        current = version.parse(gitlab_version)
        target = version.parse(self.target_version.split("-")[0])

        if current > target:
            raise RuntimeError(f"Rollback from {current} to {target} not supported!")
        if current.major < target.major:
            ask_confirmation(
                f"Doing **major** upgrade from {current} to {target}. "
                "Did you check release notes for manual migrations steps or breaking changes?")

    def fail_for_disk_space(self):
        """Available disk space must be below DISK_HIGH_THRESHOLD."""
        if get_disk_usage_for_path(self.remote_host, BACKUP_DIRECTORY) > DISK_HIGH_THRESHOLD:
            raise RuntimeError(f"Not enough disk space in {BACKUP_DIRECTORY}")

    @cached_property
    def is_replica(self) -> bool:
        """Check that we aren't running on the active host, as defined in hiera."""
        active_host = self.spicerack.puppet_server().hiera_lookup(self.fqdn, ACTIVE_HOST_HIERA_KEY).strip()
        # An empty or bogus lookup must not make the active host look like a replica
        if "." not in active_host:
            raise RuntimeError(f"Unable to look up {ACTIVE_HOST_HIERA_KEY} for {self.fqdn}: got '{active_host}'")
        logger.info("Active GitLab host according to hiera: %s", active_host)
        return active_host != self.fqdn

    def create_data_backup(self):
        """Create data backup"""
        logger.info('Schedule full data backup')
        self.remote_host.run_sync(f"{BACKUP_DIRECTORY}/gitlab-backup.sh full")
        logger.info('Full data backup complete')

    def create_config_backup(self):
        """Create config backup"""
        logger.info('Schedule config backup')
        self.remote_host.run_sync(f"{BACKUP_DIRECTORY}/gitlab-backup.sh config")
        logger.info('Config backup complete')

    def preload_debian_package(self):
        """Download new Debian package (apt-get install --download-only).

        GitLab Debian package is 1GB+ big, so it's downloaded before to minimize downtime

        """
        logger.info('Download new Debian package gitlab-ce=%s', self.target_version)
        self.remote_host.run_sync("apt-get update",
                                  f"apt-get install gitlab-ce={self.target_version} --download-only")

    @retry(
        tries=20,
        delay=timedelta(seconds=10),
        backoff_mode='constant',
        exceptions=(RuntimeError,))
    def fail_for_background_migrations(self):
        """Check for remaining background migrations"""
        logger.info('Check for remaining background migrations')
        results = self.remote_host.run_sync("gitlab-psql -t -c "
                                            "'SELECT job_class_name, table_name, column_name, job_arguments FROM "
                                            "batched_background_migrations WHERE status NOT IN(3, 6);'", is_safe=True)
        for _, output in results:
            lines = output.message().decode()
            # command returns 0 if no remaining background migrations were found
            if not lines:
                logger.info('No remaining background migrations found')
                break
            raise RuntimeError("Background migration running currently")

    @retry(
        tries=20,
        delay=timedelta(seconds=120),
        backoff_mode='constant',
        exceptions=(RemoteExecutionError,))
    def fail_for_running_backup(self):
        """Check for other running backups"""
        logger.info('Check for other running backups')
        self.remote_host.run_sync(f"[[ ! -e {BACKUP_LOCK_FILE} ]]", is_safe=True)

    def install_debian_package(self):
        """Install new Debian package (apt-get install)"""
        logger.info('Install new Debian package gitlab-ce=%s', self.target_version)
        self.remote_host.run_sync("DEBIAN_FRONTEND=noninteractive apt-get install -o "
                                  "Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold' "
                                  f"-y gitlab-ce='{self.target_version}'")
