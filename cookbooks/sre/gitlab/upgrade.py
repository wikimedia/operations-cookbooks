"""GitLab version upgrade cookbook"""

import logging
import re
from datetime import timedelta

import gitlab
from wmflib.interactive import ensure_shell_is_durable, get_secret
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from cookbooks.sre import CookbookBase, CookbookRunnerBase
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


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
            RuntimeError(f"Found the following hosts: {self.remote_host} for query {args.host}."
                         "Query must return 1 host.")
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.task_id = args.task_id
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.url = self.get_gitlab_url()
        self.version = args.version

        self.token = get_secret('GitLab API Token')
        self.gitlab_instance = gitlab.Gitlab(self.url, private_token=self.token)

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

        self.fail_for_disk_space()

    def run(self):
        """Run the cookbook."""
        self.create_data_backup()
        self.create_config_backup()
        self.preload_debian_package()
        self.fail_for_background_migrations()
        paused_runners = self.pause_runners()
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=15)):
            self.install_debian_package()
        self.unpause_runners(paused_runners)

        if self.phabricator is not None:
            self.phabricator.task_comment(self.task_id,
                                          f'GitLab instance {self.remote_host} upgraded to version {self.version}')

    def get_gitlab_url(self):
        """Fetch GitLab external_url from gitlab.rb config"""
        logger.info('Fetch GitLab external_url from gitlab.rb config')
        results = self.remote_host.run_sync("grep '^external_url ' /etc/gitlab/gitlab.rb", is_safe=True)
        for _, output in results:
            lines = output.message().decode()
            for line in lines.splitlines():
                return line.split('"')[1]

    def fail_for_disk_space(self):
        """Available disk space must be below DISK_HIGH_THRESHOLD."""
        logger.info('Checking available disk space')
        results = self.remote_host.run_sync(f"df --output=pcent {BACKUP_PATH} | tail -n1", is_safe=True)
        for _, output in results:
            lines = output.message().decode()
            for line in lines.splitlines():
                disk_usage = line.strip(' %')
                if re.match("[0-9]{1,3}", disk_usage):
                    if int(disk_usage) < DISK_HIGH_THRESHOLD:
                        break
                    raise RuntimeError(f"Not enough disk space in: {BACKUP_PATH}")
                raise RuntimeError(f"unable to extract free space from: {BACKUP_PATH}")

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
        logger.info('Download new Debian package gitlab-ce=%s', self.version)
        self.remote_host.run_sync("apt-get update",
                                  f"apt-get install gitlab-ce={self.version} --download-only")

    @retry(tries=20, delay=timedelta(seconds=10), backoff_mode='constant', exceptions=(RemoteExecutionError,))
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

    def pause_runners(self):
        """Pause all active runners"""
        active_runners = self.gitlab_instance.runners.all(scope='active', get_all=True)
        for runner in active_runners:
            runner.paused = True
            runner.save()
            logger.info('Paused %s runner', runner.id)
        return active_runners

    @retry(tries=20, delay=timedelta(seconds=10), backoff_mode='constant',
           exceptions=(gitlab.exceptions.GitlabUpdateError, gitlab.exceptions.GitlabHttpError,))
    def unpause_runners(self, paused_runners):
        """Unpause a list of runners"""
        for runner in paused_runners:
            runner.paused = False
            runner.save()
            logger.info('Unpaused %s runner', runner.id)

    def install_debian_package(self):
        """Install new Debian package (apt-get install)"""
        logger.info('Install new Debian package gitlab-ce=%s', self.version)
        self.remote_host.run_sync("DEBIAN_FRONTEND=noninteractive apt-get install -o "
                                  "Dpkg::Options::='--force-confdef' -o Dpkg::Options::='--force-confold' "
                                  f"-y gitlab-ce='{self.version}'")
