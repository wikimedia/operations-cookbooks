"""Gerrit sync-instances cookbook.

This cookbook performs a one-way data sync between two Gerrit hosts.
"""

import logging
from argparse import ArgumentParser
from wmflib.interactive import ensure_shell_is_durable, ask_input
from cookbooks.sre import CookbookBase, CookbookRunnerBase

from . import GERRIT_DIR_PREFIX

logger = logging.getLogger(__name__)


class SyncInstances(CookbookBase):
    """Syncs Gerrit data from one host to another, from scratch."""

    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parses arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--source",
            required=True,
            help="Gerrit host to sync FROM",
        )
        parser.add_argument(
            "--replica",
            required=True,
            help="Gerrit host to sync TO",
        )

        warning = (
            "adds -v to rsync"
        )
        parser.add_argument(
            "--verbose",
            required=False,
            default=False,
            action="store_true",
            help=warning,
        )
        warning = (
            "This argument is designed to handle user migration between Gerrit instances. "
            "If set, rsynced files on the replica will be chowned to the Gerrit daemon user."
        )
        parser.add_argument(
            "--chown",
            required=False,
            default=False,
            action="store_true",
            help=warning,
        )

        warning = (
            "This argument needs to be used with caution. "
            "We will distrust Gerrit's replication for Git data and rsync the Git data directory as well. "
            "Without this flag, the Git data directory (git_dir) is excluded from rsync and expected "
            "to be handled by Gerrit's replication."
        )
        parser.add_argument(
            "--distrust",
            required=False,
            default=False,
            action="store_true",
            help=warning,
        )

        return parser

    def get_runner(self, args):
        """Creates Spicerack runner."""
        return SyncInstancesRunner(args, self.spicerack)


# pylint: disable=too-many-instance-attributes
class SyncInstancesRunner(CookbookRunnerBase):
    """Runner class for executing SyncInstances."""

    # Customize Cookbook lock
    max_concurrency = 1
    lock_ttl = 3600

    def __init__(self, args, spicerack) -> None:
        """Initialize runner."""
        ensure_shell_is_durable()

        self.spicerack = spicerack
        self.args = args

        # Remote host groups
        self.switch_from_host = spicerack.remote().query(f"{args.source}.*")
        self.switch_to_host = spicerack.remote().query(f"{args.replica}.*")

        self.message = f"sync Gerrit data from {self.switch_from_host} to {self.switch_to_host}"

        # For hiera lookups
        self.puppetserver = spicerack.puppet_server()
        # Hieradata lookups
        self.target_gerrit_user = self.puppetserver.hiera_lookup(
            self.switch_to_host.hosts[0],
            "profile::gerrit::daemon_user",
        ).splitlines()[-1]
        logger.info("Retrieved target Gerrit user: %s", self.target_gerrit_user)

        self.source_gerrit_site = self.puppetserver.hiera_lookup(
            self.switch_from_host.hosts[0],
            "profile::gerrit::gerrit_site",
        ).splitlines()[-1]
        logger.info("Retrieved source Gerrit site: %s", self.source_gerrit_site)

        self.target_gerrit_site = self.puppetserver.hiera_lookup(
            self.switch_to_host.hosts[0],
            "profile::gerrit::gerrit_site",
        ).splitlines()[-1]
        logger.info("Retrieved target Gerrit site: %s", self.target_gerrit_site)

        # Used when we want to exclude the Git data dir if we trust replication
        self.src_git_dir = self.puppetserver.hiera_lookup(
            self.switch_from_host.hosts[0],
            "profile::gerrit::git_dir",
        ).splitlines()[-1]
        logger.info(
            "Retrieved source Gerrit data dir for potential rsync exclusion: %s",
            self.src_git_dir,
        )

    @property
    def runtime_description(self) -> str:
        """Returns a nicely formatted message describing what we're doing."""
        return self.message

    def confirm_before_proceeding(self) -> None:
        """Make sure the user knows what the cookbook will do."""
        ask_input(
            (
                "This will rsync Gerrit data from "
                f"{self.args.source} to {self.args.replica}.\n"
                "Make sure Gerrit writes are quiesced on the source before continuing.\n"
                f"To proceed, type: {self.args.replica}"
            ),
            choices=[self.args.replica],
        )

    def confirm_distrust_mode(self) -> None:
        """Additional confirmation when running with --distrust."""
        if not self.args.distrust:
            return
        ask_input(
            (
                "You requested --distrust.\n"
                "This will rsync the Git data directory as well, instead of relying on Gerrit's replication.\n"
                "Type 'erase-everything-on-target' to confirm you really want to do this."
            ),
            choices=["erase-everything-on-target"],
        )

    def run(self) -> None:
        """Entrypoint to execute cookbook."""
        logger.info("Starting Gerrit sync-instances: %s", self.message)
        self.confirm_distrust_mode()
        self.confirm_before_proceeding()

        # If we distrust replication, we rsync all directories (including git_dir).
        # Otherwise, we exclude git_dir and rely on Gerrit replication for it.
        all_dirs = self.args.distrust

        self.sync_files(all_dirs=all_dirs)

        logger.info("Gerrit sync-instances cookbook completed successfully.")

    def sync_files(self, all_dirs: bool = False) -> bool:
        """Transfers files from old to new Gerrit host."""
        logger.info("Starting to rsync to %s.", self.args.replica)

        base_rsync_args = " -apz --stats --delete "
        if self.args.verbose:
            logger.info("Using -vP to increase verbosity")
            base_rsync_args += " -vP"

        if self.args.chown:
            logger.info("Using --no-o --no-g,  chown will be run separately.")
            base_rsync_args += " --no-o --no-g"

        if self.spicerack.dry_run:
            base_rsync_args += " --dry-run"
            logger.info(
                "Running in dry-run mode: rsync commands will use --dry-run and "
                "won't modify data on %s.",
                self.args.replica,
            )

        # /var/lib/gerrit (site data)
        command_sync_var_lib = (
            f"/usr/bin/rsync {base_rsync_args} {self.source_gerrit_site}/ "
            f"rsync://{self.args.replica}/gerrit-var-lib/"
        )

        # /srv/gerrit (data dir)
        command_sync_data = (
            f"/usr/bin/rsync {base_rsync_args} /srv/gerrit/ "
            f"rsync://{self.args.replica}/gerrit-data/ "
            "--exclude=*.hprof "
        )
        if not all_dirs:
            # Trust Gerrit replication for Git data: exclude git_dir
            logger.info(
                "Excluding %s from rsync.",
                self.src_git_dir,
            )
            command_sync_data += f"--exclude {self.src_git_dir} "
            logger.info(
                "Adding protect filter to rsync args",
            )

            # TODO double check if protect path is relative or absolute
            command_sync_data += "--filter='protect /srv/gerrit/git/' "
            command_sync_data += "--filter='protect git/' "
            #  Double protection against accidental deletion of git data
            # https://linux.die.net/man/1/rsync#:~:text=%2D%2Dfilter%20%27protect%20emptydir%2F%27

        # If we distrust replication: rsync everything under /srv/gerrit

        logger.info("Running rsync on /var/lib data: %s", command_sync_var_lib)
        self.confirm_before_proceeding()
        self.switch_from_host.run_sync(
            command_sync_var_lib,
            print_progress_bars=False,
            print_output=True,
            is_safe=True,
        )

        logger.info("Running rsync on git/data dir: %s", command_sync_data)
        self.confirm_before_proceeding()
        self.switch_from_host.run_sync(
            command_sync_data,
            print_progress_bars=False,
            print_output=True,
            is_safe=True,
        )

        if self.args.chown:
            cmd = (
                f"chown -R {self.target_gerrit_user}:{self.target_gerrit_user} "
                f"{GERRIT_DIR_PREFIX} {self.target_gerrit_site}"
            )
            logger.info("chowning files as --chown has been passed. Will use the following:")
            logger.info(cmd)
            if self.spicerack.dry_run:
                logger.info(
                    "Would have run chown command %s (skipped due to dry-run mode) on %s.",
                    cmd,
                    self.args.replica,
                )
            else:
                self.switch_to_host.run_sync(
                    cmd,
                    print_progress_bars=False,
                    print_output=False,
                    is_safe=False,
                )

        return True
