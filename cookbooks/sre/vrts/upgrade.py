"""VRTS Upgrade Cookbook"""

import logging

from packaging import version
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.vrts import get_current_version

logger = logging.getLogger(__name__)

ENV_FILE_PATH = "/etc/vrts/install-script-vars"
DOWNLOAD_URL = "https://download.znuny.org/releases"
VRTS_USER = "otrs"


class Upgrade(CookbookBase):
    """Upgrade VRTS hosts to a new version

    Usage example:
        cookbook sre.vrts.ugprade --version 6.5.6 -r "Latest LTS Version" -t T12345 vrts1001
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self):
        """Parses arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--version", required=True, help="Version of new VRTS installation"
        )
        parser.add_argument(
            "host",
            help="Short hostname of the VRTS host to upgrade e.g. vrts1001",
        )
        return parser

    def get_runner(self, args):
        """Create spicerack runner"""
        return UpgradeRunner(args, self.spicerack)


class UpgradeRunner(CookbookRunnerBase):
    """Upgrade a VRTS host to a new version"""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.host = args.host
        self.proxy = spicerack.http_proxy
        self.remote_host = spicerack.remote().query(f"{args.host}.*")
        if len(self.remote_host) != 1:
            raise RuntimeError(
                f"Found the following hosts: {self.remote_host} for query {args.host}. Query must return 1 host"
            )
        self.puppet_host = spicerack.puppet(self.remote_host)
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.task_id = args.task_id
        self.current_version = get_current_version(self.remote_host)
        self.target_version = args.version
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        self.message = f" on VRTS host {self.remote_host}"

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action"""
        return self.message

    @property
    def lock_args(self):
        """Make the cookbook lock exclusive per-host."""
        return LockArgs(suffix=self.host, concurrency=1, ttl=7200)

    def rollback(self):
        """Add a comment to the Phabricator task if cookbook failed"""
        self.phabricator.task_comment(
            self.task_id,
            f"Cookbook {__name__} started by {self.admin_reason.owner} executed with errors "
            f"{self.runtime_description}\n",
        )

    def run(self):
        """Run the cookbook"""
        self.phabricator.task_comment(
            self.task_id,
            f"Cookbook {__name__} was started by {self.admin_reason.owner} {self.runtime_description}",
        )

        ask_confirmation(
            f"This will upgrade VRTS on {self.host} from {self.current_version} to {self.target_version}. "
            "Kindly confirm that this is what you intend to do."
        )

        with self.puppet_host.disabled(self.admin_reason):
            self.download_vrts()
            self.extract_vrts()
            self.stop_services()
            self.copy_files()
            self.symlink()
            self.configure_install()
            self.start_services()
            self.cleanup()

        # Run Puppet
        self.puppet_host.run()

    def download_vrts(self):
        """Download VRTS"""
        logger.info("Downloading VRTS")
        current_version = get_current_version(self.remote_host)
        if version.parse(self.target_version) < current_version:
            raise RuntimeError("Version must be greater than current version")
        self.remote_host.run_sync(
            f"runuser -u {VRTS_USER} -- "
            f"/usr/bin/curl -x {self.proxy} -L {DOWNLOAD_URL}/znuny-{self.target_version}.tar.gz "
            f"-o /tmp/znuny-{self.target_version}.tar.gz"
        )

    def extract_vrts(self):
        """Extract VRTS"""
        logger.info("Extracting VRTS")
        self.remote_host.run_sync(
            f"/usr/bin/tar xfz /tmp/znuny-{self.target_version}.tar.gz -C /opt"
        )

    def stop_services(self):
        """Disable services on host"""
        logger.info("Disabling all VRTS services on host")
        self.remote_host.run_sync("systemctl stop exim4 apache2 vrts-daemon")

    def start_services(self):
        """Enable services on host"""
        logger.info("Enabling all disabled services")
        self.remote_host.run_sync("systemctl start exim4 apache2 vrts-daemon")

    def copy_files(self):
        """Copy over old config files"""
        logger.info("Copying configuration files")
        self.remote_host.run_sync(
            f"cp /opt/otrs/Kernel/Config.pm /opt/znuny-{self.target_version}/Kernel",
        )

    def symlink(self):
        """Create symbolic link pointing to new version"""
        logger.info("Symlinking to new version")
        self.remote_host.run_sync(
            f"ln -sfnv /opt/znuny-{self.target_version} /opt/otrs"
        )

    def configure_install(self):
        """Configure installation"""
        self.remote_host.run_sync(
            "/opt/otrs/bin/otrs.SetPermissions.pl --web-group=www-data",
            f"runuser -u {VRTS_USER} -- /opt/otrs/bin/otrs.Console.pl Maint::Config::Rebuild",
            f"runuser -u {VRTS_USER} -- /opt/otrs/bin/otrs.Console.pl Maint::Cache::Delete",
        )
        # self.remote_host.run_sync(
        #     "/opt/otrs/bin/otrs.Console.pl Admin::Package::ReinstallAll",
        #     user="www-data",
        # )

    def cleanup(self):
        """Remove download files from /tmp"""
        # TODO: Remove Old VRTS Installations?
        self.remote_host.run_sync(f"rm -f /tmp/znuny-{self.target_version}.tar.gz")
