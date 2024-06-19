"""Cookbook to migrate hosts from puppet5 to the puppet7 environment"""

from datetime import timedelta
from logging import getLogger
from textwrap import dedent

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.remote import RemoteError
from wmflib.interactive import (
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
)


class MigrateRole(CookbookBase):
    """Migrate host to puppet 7 environment

    Usage:
        cookbook sre.puppet.migrate-role sretest1001
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()

        parser.add_argument("role", help="The host to migrate.")
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return MigrateRoleRunner(args, self.spicerack)


class MigrateRoleRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the MigrateRole runner."""
        ensure_shell_is_durable()
        self.dry_run = spicerack.dry_run
        self.role = args.role
        self.logger = getLogger(__name__)
        query = f"P{{O:{self.role}}} and P{{F:puppetversion ~ '^5\\.' and not F:lsbmajdistrelease = '10'}}"
        try:
            self.remote_hosts = spicerack.remote().query(query)
        except RemoteError as error:
            raise RuntimeError("No hosts found matching {self.role} still running puppet5") from error

        ask_confirmation(f'Please confirm the list of Puppet 5 hosts to convert, ok to proceed? {self.remote_hosts}')

        self.puppet = spicerack.puppet(self.remote_hosts)
        if len(self.remote_hosts) == 0:
            raise RuntimeError(f"All host matching the role {self.role} already seem to be running puppet7")
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_hosts.hosts)
        self.apt_get = spicerack.apt_get(self.remote_hosts)
        self.puppet_master = spicerack.puppet_master()
        self.puppet_server = spicerack.puppet_server()
        self.reason = spicerack.admin_reason(f'Migrating {self.role} to puppet7')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for role: {self.role}'

    @retry(
        backoff_mode='constant', delay=timedelta(seconds=1), exceptions=(RuntimeError,)
    )
    def update_hiera(self):
        """Inform the user to manually update the hiera config and check this has been performed."""
        ask_confirmation(
            dedent(
                f"""\
            Please add the following hiera entry to:

            hieradata/role/common/{self.role.replace('::', '/')}.yaml
                profile::puppet::agent::force_puppet7: true

            Press continue when the change is merged
            """
            )
        )
        common_msg = (
            "Please ensure you have merged the above change and puppet ran successfully"
        )
        self.puppet.run()
        self.apt_get.install('puppet-agent')
        versions = self.remote_hosts.run_async("puppet --version", is_safe=True)
        try:
            for host, version in versions:
                if int(version.message().decode().split('.')[0]) != 7:
                    raise RuntimeError(
                        f"{host}: the puppet version {version} is not 7. {common_msg}"
                    )
        except ValueError as err:
            raise RuntimeError(f"Major version: {version}.  Please double check the hiera change") from err
        use_srv_records = self.remote_hosts.run_async(
            "puppet config --section agent print use_srv_records", is_safe=True
        )
        for host, use_srv_record in use_srv_records:
            if use_srv_record.message().decode() != "true":
                raise RuntimeError(
                    f"{host}: use_srv_records is not enabled. {common_msg}.  Please double check the hiera change"
                )

    def rollback(self):
        """Rollback actions."""
        self.remote_hosts.run_sync('rm -f /run/puppet/disabled')
        print("The cookbook has failed you will need to manually investigate the state.")

    def run(self):
        """Main run method either query or clear MigrateRole events."""
        with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
            # Stop any runs that have already started
            self.remote_hosts.run_sync('systemctl stop puppet-agent-timer.service')
            self.remote_hosts.run_sync('touch /run/puppet/disabled')
            self.update_hiera()
            fingerprints = self.puppet.regenerate_certificate()
            for fqdn, fingerprint in fingerprints.items():
                self.puppet_server.sign(fqdn, fingerprint)
            confirm_on_failure(self.puppet.run)
            # Clean up the certs on the old puppet master
            for fqdn in fingerprints.keys():
                self.puppet_master.destroy(fqdn)
            self.remote_hosts.run_sync('rm -f /run/puppet/disabled')
