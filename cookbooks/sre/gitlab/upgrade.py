"""GitLab version upgrade cookbook"""

import logging
from datetime import timedelta
from packaging import version

import gitlab
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable, get_secret
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from cookbooks.sre import CookbookBase, CookbookRunnerBase
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.gitlab import get_gitlab_url, get_disk_usage_for_path, pause_runners, unpause_runners


BACKUP_PATH = "/srv/gitlab-backup"
DISK_HIGH_THRESHOLD = 70

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
    - Install new Debian gitlab-ce package
    - Wait for GitLab and Unpause Runners

    Usage example:
        cookbook sre.gitlab.upgrade --host gitlab1004 --version 15.4.4-ce.0 -r 'some reason' -t T12345

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--host', help='Short hostname of the gitlab host to upgrade, not FQDN')
        parser.add_argument('--version', help='Version of new GitLab Debian package in Debian versioning schema')
        parser.add_argument('-r', '--reason', required=True,
                            help=('The reason for the downtime. The current username and originating host are '
                                  'automatically added.'))
        parser.add_argument('-t', '--task-id', required=False,
                            help='An optional task ID to refer in the downtime message (i.e. T12345).')
        return parser

    batch_default = 1

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UpgradeRunner(args, self.spicerack)


class UpgradeRunner(CookbookRunnerBase):
    """Upgrade a GitLab host to a new version."""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.remote_host = spicerack.remote().query(f'{args.host}.*')
        if len(self.remote_host) != 1:
            raise RuntimeError(f"Found the following hosts: {self.remote_host} for query {args.host}."
                               "Query must return 1 host.")
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.task_id = args.task_id
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.url = get_gitlab_url(self.remote_host)
        self.target_version = args.version

        self.token = get_secret('GitLab API Token')
        self.gitlab_instance = gitlab.Gitlab(self.url, private_token=self.token)

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

        self.check_gitlab_version()
        self.fail_for_disk_space()

        self.message = f'on GitLab host {self.remote_host} with reason: {args.reason}'

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return self.message

    def rollback(self):
        """Comment on phabricator in case of a failed run."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                f"Cookbook {__name__} started by {self.admin_reason.owner} executed with errors:\n"
                f"{self.runtime_description}\n"
            )

    def run(self):
        """Run the cookbook."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                f'Cookbook {__name__} was started by {self.admin_reason.owner} {self.runtime_description}')

        broadcastmessage = self.gitlab_instance.broadcastmessages.create({
            'message': f'Maintenance {self.message} starting soon.',
            'broadcast_type': 'notification'
        })
        self.preload_debian_package()
        self.create_data_backup()
        self.create_config_backup()
        self.fail_for_background_migrations()
        paused_runners = pause_runners(self.token, self.url)
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=15)):
            self.install_debian_package()
        unpause_runners(paused_runners)
        broadcastmessage.delete()

        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id,
                f'Cookbook {__name__} started by {self.admin_reason.owner} {self.runtime_description} completed '
                f'successfully {self.runtime_description}')

    def check_gitlab_version(self):
        """Compare current GitLab version with target version.

        Also prevent downgrade and ask confirmation for major upgrades.

        """
        logger.info('Get GitLab version from API')

        gitlab_version = self.gitlab_instance.version()[0]
        if gitlab_version == "unknown":
            raise RuntimeError("Failed to get GitLab version from API, check API token and URL")

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
        if get_disk_usage_for_path(self.remote_host, BACKUP_PATH) > DISK_HIGH_THRESHOLD:
            raise RuntimeError(f"Not enough disk space in {BACKUP_PATH}")

    def create_data_backup(self):
        """Create data backup"""
        logger.info('Schedule full data backup')
        self.remote_host.run_sync(f"{BACKUP_PATH}/gitlab-backup.sh full")
        logger.info('Full data backup complete')

    def create_config_backup(self):
        """Create config backup"""
        logger.info('Schedule config backup')
        self.remote_host.run_sync(f"{BACKUP_PATH}/gitlab-backup.sh config")
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
        exceptions=(RemoteExecutionError,))
    def fail_for_background_migrations(self):
        """Check for remaining background migrations"""
        logger.info('Check for remaining background migrations')
        results = self.remote_host.run_sync("gitlab-rails runner -e production "
                                            "'puts Gitlab::BackgroundMigration.remaining'", is_safe=True)
        for _, output in results:
            lines = output.message().decode()
            # command returns 0 if no remaining background migrations were found
            if lines[0] == "0":
                logger.info('No remaining background migrations found')
                break
            raise RuntimeError("Background migration running currently")

    def install_debian_package(self):
        """Install new Debian package (apt-get install)"""
        logger.info('Install new Debian package gitlab-ce=%s', self.target_version)
        self.remote_host.run_sync("DEBIAN_FRONTEND=noninteractive apt-get install -o "
                                  "Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold' "
                                  f"-y gitlab-ce='{self.target_version}'")
