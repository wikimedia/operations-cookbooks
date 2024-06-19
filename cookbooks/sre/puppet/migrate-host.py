"""Cookbook to migrate hosts from puppet5 to the puppet7 environment"""

from datetime import timedelta
from logging import getLogger
from textwrap import dedent

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.remote import RemoteError
from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable


class MigrateHosts(CookbookBase):
    """Migrate host to puppet 7 environment

    Usage:
        cookbook sre.puppet.migrate-hosts sretest1001
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("--no-downtime", action="store_true",
                            help="Do not downtime the host during the migration.")
        parser.add_argument("fqdn", help="The host to migrate.")
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return MigrateHostsRunner(args, self.spicerack)


class MigrateHostsRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the MigrateHosts runner."""
        ensure_shell_is_durable()
        self.dry_run = spicerack.dry_run
        self.fqdn = args.fqdn
        self.no_downtime = args.no_downtime
        self.logger = getLogger(__name__)
        try:
            self.remote_host = spicerack.remote().query(self.fqdn)
        except RemoteError as error:
            raise RuntimeError("No hosts found matching {args.fqdn}") from error

        if len(self.remote_host) == 0:
            raise RuntimeError('Specified server not found, bailing out')

        if len(self.remote_host) != 1:
            raise RuntimeError('Only a single server can be migrated')

        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)
        self.apt_get = spicerack.apt_get(self.remote_host)
        self.puppet = spicerack.puppet(self.remote_host)
        self.puppet_master = spicerack.puppet_master()
        self.puppet_server = spicerack.puppet_server()
        self.reason = spicerack.admin_reason(f'Migrating {self.fqdn} to puppet7')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for host {self.fqdn}'

    def _get_output(self, command: str) -> str:
        """Run a command on the host and return the output"""
        results = self.remote_host.run_async(command, is_safe=True)
        try:
            _, result = next(results)
        except StopIteration as err:
            raise RuntimeError(f"no results for {command}") from err
        return result.message().decode()

    @retry(backoff_mode='constant', delay=timedelta(seconds=1), exceptions=(RuntimeError,))
    def update_hiera(self):
        """Inform the user to manually update the hiera config and check this has been performed."""
        host = self.fqdn.split('.')[0]
        ask_confirmation(dedent(
            f"""\
            Please add the following hiera entry to:

            hieradata/hosts/{host}.yaml
                profile::puppet::agent::force_puppet7: true

            Press continue when the change is merged
            """
        ))
        common_msg = "Please ensure you have merged the above change and puppet ran successfully"
        self.puppet.run()
        self.apt_get.install('puppet-agent')
        version = self._get_output("puppet --version")
        try:
            if int(version.split('.')[0]) != 7:
                raise RuntimeError(f"the puppet version {version} is not 7. {common_msg}")
        except ValueError as err:
            raise RuntimeError(f"Major version: {version}.  Please double check the hiera change") from err
        use_srv_records = self._get_output("puppet config --section agent print use_srv_records")
        if use_srv_records != "true":
            raise RuntimeError(f"use_srv_records is not enabled. {common_msg}.  Please double check the hiera change")

    def rollback(self):
        """Rollback actions."""
        self.remote_host.run_sync('rm -f /run/puppet/disabled')
        print("The cookbook has failed you will need to manually investigate the state.")

    def run(self):
        """Main run method either query or clear MigrateHosts events."""
        if self.no_downtime:
            self._run()
        else:
            with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
                self._run()

    def _run(self):
        """Run all the commands."""
        # Stop any runs that have already started
        self.remote_host.run_sync('systemctl stop puppet-agent-timer.service')
        self.remote_host.run_sync('touch /run/puppet/disabled')
        self.update_hiera()
        fingerprints = self.puppet.regenerate_certificate()
        self.puppet_server.sign(self.fqdn, fingerprints[self.fqdn])
        confirm_on_failure(self.puppet.run)
        # Clean up the certs on the old puppet master
        self.puppet_master.destroy(self.fqdn)
        self.remote_host.run_sync('rm -f /run/puppet/disabled')
