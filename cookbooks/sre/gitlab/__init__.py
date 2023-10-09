"""GitLab Cookbooks"""

import logging
import re

from datetime import timedelta

import gitlab

from spicerack.decorators import retry
from spicerack.remote import RemoteHosts

__title__ = __doc__
logger = logging.getLogger(__name__)


def get_gitlab_url(host: RemoteHosts) -> str:
    """Fetch GitLab external_url from gitlab.rb config"""
    logger.info('Fetch GitLab external_url from gitlab.rb config')
    results = host.run_sync("grep '^external_url ' /etc/gitlab/gitlab.rb", is_safe=True, print_progress_bars=False)
    for _, output in results:
        lines = output.message().decode()
        for line in lines.splitlines():
            return line.split('"')[1]
    raise RuntimeError("Could not retrieve external_url from #{host}")


def get_disk_usage_for_path(host: RemoteHosts, path: str) -> int:
    """Fetches the output of `df` on the path provided"""
    logger.info("Checking available disk space on %s", host)
    results = host.run_sync(f"df --output=pcent {path} | tail -n1", is_safe=True, print_progress_bars=False)
    for _, output in results:
        lines = output.message().decode()
        for line in lines.splitlines():
            disk_usage = line.strip(' %')
            if re.match("[0-9]{1,3}", disk_usage):
                return int(disk_usage)
    raise RuntimeError(f"Unable to extract free space from: {path}")


def pause_runners(token: str, url: str, dry_run: bool = True):
    """Pause all active runners"""
    gitlab_instance = gitlab.Gitlab(url, private_token=token)
    active_runners = gitlab_instance.runners.all(scope='active', all=True)
    paused_runners = []
    for runner in active_runners:
        if not dry_run:
            try:
                runner.paused = True
                runner.save()
            except (gitlab.exceptions.GitlabHttpError, gitlab.exceptions.GitlabUpdateError) as caught_exception:
                logger.error("Failed to pause runner %s with error %s", runner, caught_exception.error_message)
                continue
        paused_runners.append(runner)
        logger.info('Paused %s runner', runner.id)
    return paused_runners


@retry(
    tries=20,
    delay=timedelta(seconds=10),
    backoff_mode='constant',
    failure_message='Waiting for GitLab API to become available again',
    exceptions=(gitlab.exceptions.GitlabUpdateError, gitlab.exceptions.GitlabHttpError,))
def unpause_runners(paused_runners, dry_run=True):
    """Unpause a list of runners"""
    for runner in paused_runners:
        if not dry_run:
            runner.paused = False
            runner.save()
        logger.info('Unpaused %s runner', runner.id)
