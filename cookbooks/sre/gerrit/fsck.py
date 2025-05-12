"""Gerrit backup git tree fsck cookbook.

This cookbook manages Gerrit backup tree consistency.
"""
from __future__ import annotations

import logging
import shlex
from argparse import ArgumentParser
from datetime import timedelta
from pathlib import PurePosixPath
from typing import Set

from wmflib.interactive import ensure_shell_is_durable, ask_confirmation
from cookbooks.sre import CookbookBase, CookbookRunnerBase, PHABRICATOR_BOT_CONFIG_FILE

from . import GERRIT_BACKUP_PREFIX, GERRIT_DIRS

logger = logging.getLogger(__name__)


class FsckBackup(CookbookBase):
    """CLI entrypoint for the cookbook targeting a single host."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Define CLI arguments for the cookbook."""
        parser = super().argument_parser()
        parser.add_argument(
            "--host",
            required=True,
            help="Host on which to run git fsck",
        )
        return parser

    def get_runner(self, args):
        """Return the runner instance for given arguments."""
        return FsckBackupRunner(args, self.spicerack)


class FsckBackupRunner(CookbookRunnerBase):
    """Runner that executes fsck for a single host."""

    def __init__(self, args, spicerack):
        """Initialize runner with arguments and configure environment."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.host_pattern = args.host
        self.host_query = spicerack.remote().query(self.host_pattern + ".*")
        self.host_name = args.host

        self.message = (
            f"git fsck on local backups on {self.host_name} to ensure consistency."
        )

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.reason = spicerack.admin_reason(reason=self.message)

        # Confirm with the operator
        self.confirm_before_proceeding()

    @property
    def runtime_description(self) -> str:
        """Return a human-readable description of the fsck operation."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Ask user to confirm before proceeding with the fsck operation."""
        ask_confirmation(
            (
                f"This will run \"git fsck --strict\" against all repositories under "
                f"{GERRIT_BACKUP_PREFIX} on host: {self.host_name}. "
                "It can take a long time on large repos. Continue?"
            )
        )

    def run(self) -> None:
        """Execute fsck process on the configured host."""
        hosts: Set = set(self.host_query.hosts)

        alerting_hosts = self.spicerack.alerting_hosts(hosts)
        alerting_hosts.downtime(self.reason, duration=timedelta(hours=4))

        self._fsck_host(self.host_query)

        logger.info(
            "Backup repositories passed git fsck on host %s", self.host_name
        )

    def _fsck_host(self, hostset) -> None:
        """Run fsck on all discovered repositories for the host."""
        host = hostset.hosts[0]
        logger.info("→ Checking local backups consistency on %s", host.name)

        for directory in GERRIT_DIRS:
            backup_root = PurePosixPath(GERRIT_BACKUP_PREFIX) / directory
            repos = self._discover_repositories(hostset, str(backup_root))

            if not repos:
                logger.warning(
                    "No repositories found under %s on %s", backup_root, host.name
                )
                continue

            for repo_path in repos:
                self._git_fsck_repo(hostset, repo_path)

    def _discover_repositories(self, hostset, root: str) -> list[str]:
        """Find both standard and bare Git repositories under the root path."""
        git_dirs = self._remote_fd_list(hostset, root, "--type", "d", "--name", "*.git")
        head_files = self._remote_fd_list(hostset, root, "--type", "f", "--name", "HEAD")

        bare_repos = {str(PurePosixPath(path).parent) for path in head_files}
        all_repos = set(git_dirs) | bare_repos

        sorted_repos = sorted(all_repos)
        logger.debug("Discovered %d repos under %s", len(sorted_repos), root)
        return sorted_repos

    def _remote_fd_list(self, hostset, root: str, *args) -> list[str]:
        """Run fdfind remotely and return matching absolute paths."""
        cmd_parts = [
            "fdfind",
            "--hidden",
            "--absolute-path",
            *args,
            "",
            shlex.quote(root),
        ]
        cmd = " ".join(cmd_parts)

        result = hostset.run_sync(cmd, print_progress_bars=False, print_output=False)
        paths: list[str] = []
        for _host, output in result:
            text = output.message().decode().strip()
            if text:
                for line in text.split("\n"):
                    if line:
                        paths.append(line)
        return paths

    def _git_fsck_repo(self, hostset, repo_path: str) -> None:
        """Run 'git fsck --strict' on a given repository and fail if errors occur."""
        host = hostset.hosts[0]
        logger.debug("[fsck] %s: %s", host.name, repo_path)

        cmd = f"git -C {shlex.quote(repo_path)} fsck --strict"
        results = hostset.run_sync(cmd, print_progress_bars=False, print_output=True)

        for _, output in results:
            if output.failed:
                raise RuntimeError(
                    f"git fsck failed on {repo_path} of host {host.name}. See logs for details."
                )
