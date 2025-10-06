# cookbooks/sre/gerrit/localbackup.py
"""Cookbook to create/refresh local emergency backups on Gerrit hosts."""

import logging
from argparse import ArgumentParser
from typing import Iterable

from wmflib.interactive import ensure_shell_is_durable, ask_input
from cookbooks.sre import CookbookBase, CookbookRunnerBase
from . import GERRIT_DIR_PREFIX, GERRIT_BACKUP_PREFIX, GERRIT_DIRS

logger = logging.getLogger(__name__)


class LocalBackup(CookbookBase):
    """Create or refresh the local backups on the given Gerrit hosts."""

    # Keep this cookbook runnable without an external task id.
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parse command-line arguments for this cookbook."""
        parser = super().argument_parser()

        parser.add_argument(
            "--source",
            required=True,
            help="Primary host (cumin pattern accepted, e.g. gerrit1003).",
        )
        parser.add_argument(
            "--replica",
            required=False,
            help="Optional secondary host (e.g. gerrit2003). If set, run on both.",
        )

        # Optional overrides.
        parser.add_argument(
            "--dir-prefix",
            default=GERRIT_DIR_PREFIX,
            help=f"Source prefix. Default: {GERRIT_DIR_PREFIX!s}",
        )
        parser.add_argument(
            "--backup-prefix",
            default=GERRIT_BACKUP_PREFIX,
            help=f"Destination prefix. Default: {GERRIT_BACKUP_PREFIX!s}",
        )
        parser.add_argument(
            "--dirs",
            default=",".join(GERRIT_DIRS),
            help=f"Comma-separated subdirs. Default: {','.join(GERRIT_DIRS)}",
        )

        return parser

    def get_runner(self, args):
        """Create the Spicerack runner."""
        return LocalBackupRunner(args, self.spicerack)


class LocalBackupRunner(CookbookRunnerBase):
    """Runner class for executing local Gerrit backups."""

    # Conservative values for locking; local rsync is lightweight.
    max_concurrency = 1
    lock_ttl = 7200

    def __init__(self, args, spicerack) -> None:
        """Initialize runner with targets and parameters."""
        ensure_shell_is_durable()
        self.spicerack = spicerack
        self.args = args

        # Resolve cumin patterns like 'gerrit1003.*'
        self.source = spicerack.remote().query(f"{args.source}.*")
        self.replica = spicerack.remote().query(f"{args.replica}.*") if args.replica else None

        # Normalize prefixes and parse dirs list.
        self.dir_prefix = args.dir_prefix.rstrip("/") + "/"
        self.backup_prefix = args.backup_prefix.rstrip("/") + "/"
        self.dirs: Iterable[str] = [d.strip().strip("/") for d in args.dirs.split(",") if d.strip()]

        host_list = list(self.source.hosts) + (list(self.replica.hosts) if self.replica else [])
        self.message = f"Prepare local backup on: {', '.join(host_list)}"
        logger.info("Will run local backup on: %s", ", ".join(host_list))

        # Small human confirmation before writing anything.
        ask_input(
            f"This will create/refresh local backups under {self.backup_prefix} on: {', '.join(host_list)}.\n"
            "Type 'backup' to proceed.",
            choices=["backup"],
        )

    @property
    def runtime_description(self) -> str:
        """Return a short, human-friendly description for 'cookbook' status output."""
        return self.message

    def run(self) -> None:
        """Entrypoint to execute the cookbook."""
        hosts = [self.source] + ([self.replica] if self.replica else [])
        for host in hosts:
            logger.info("Preparing local backup on %s", host)
            self._backup_dirs_on_host(host)

        logger.info("Local backup completed successfully.")

    def _backup_dirs_on_host(self, host) -> None:
        """Create destination dirs and run local rsync on a single host (idempotent)."""
        for directory in self.dirs:
            src = f"{self.dir_prefix}{directory}/"
            dst = f"{self.backup_prefix}{directory}/"

            # Idempotent mkdir on the target host.
            host.run_sync(
                f"/bin/mkdir -p {dst}",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )

            # Same rsync flags as in the failover cookbook.
            if not self.spicerack.dry_run:
                rsync_cmd = (
                    "/usr/bin/rsync -av --delete-before "
                    f"{src} {dst}"
                )
            else:
                rsync_cmd = (
                    "/usr/bin/rsync -av --dry-run --delete-before "
                    f"{src} {dst}"
                )
            logger.info("Running backup rsync on %s: %s", host, rsync_cmd)

            host.run_sync(
                rsync_cmd,
                print_progress_bars=False,
                print_output=True,
                is_safe=True,
                #  safe → if --dry-run is used:
                #   we'll do a rsync --dry-run -iv instead of -av
            )

            if self.spicerack.dry_run:
                logger.info("Would have run backup rsync on %s: %s", host, rsync_cmd)
