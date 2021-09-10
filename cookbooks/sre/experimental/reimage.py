"""Image or re-image a physical host."""
import argparse
import logging
import os
import time

from datetime import datetime

import requests

from cumin.transports import Command
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.exceptions import SpicerackError
from spicerack.puppet import PuppetMasterError
from spicerack.remote import RemoteExecutionError
from wmflib.interactive import confirm_on_failure, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts.downtime import Downtime


logger = logging.getLogger(__name__)


class Reimage(CookbookBase):
    """Image or re-image a physical host.

    All data will be lost unless a specific partman recipe to retain partition data is used.

    Usage:
        cookbook sre.experimental.reimage -t T12345 example1001.eqiad.wmnet
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            '--no-verify', action='store_true',
            help='do not fail if hosts verification fails, just log it. It is included if --new is also set.')
        parser.add_argument(
            '--no-downtime', action='store_true',
            help=('do not set the host in downtime on Icinga before the reimage. Included if --new is set. The host '
                  'will be downtimed after the reimage in any case.'))
        parser.add_argument(
            '--no-pxe', action='store_true',
            help=('do not reboot into PXE and reimage. To be used when the reimage had issues and was manually fixed '
                  'after the timeout hence the run failed.'))
        parser.add_argument(
            '--new', action='store_true',
            help=('for first imaging of new hosts that are not in yet in Puppet and this is their first'
                  'imaging. Skips some steps prior to the reimage, includes --no-verify.'))
        parser.add_argument(
            '-c', '--conftool', action='store_true',
            help=("Depool the host via Conftool with the value of the --conftool-value option. "
                  "If the --conftool-value option is not set, its default value of 'inactive' will be "
                  "used. The host will NOT be repooled automatically, but the repool commands will "
                  "be printed at the end. If --new is also set, it will just print the pool message "
                  "at the end."))
        parser.add_argument(
            '--conftool-value', default='inactive',
            help=("Value to pass to the 'set/pooled' command in conftool to depool the host(s), if "
                  "the -c/--conftool option is set. [default: inactive]"))
        parser.add_argument(
            '--mask',
            type=lambda x: x.split(','),
            default=[],
            help=('Comma separated list of names of systemd services to mask before the first Puppet '
                  'run, without the .service suffix. Useful when some Debian package installed '
                  'starts/enable a production service before the host is ready.'))
        parser.add_argument('--httpbb', action='store_true',
                            help='run HTTP tests (httpbb) on the host after the reimage.')
        parser.add_argument('-t', '--task-id', help='the Phabricator task ID to update and refer (i.e.: T12345)')
        parser.add_argument('host', help='Short hostname of the host to be reimaged, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ReimageRunner(args, self.spicerack)


class ReimageRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the reimage runner."""
        ensure_shell_is_durable()
        self.args = args

        self.netbox = spicerack.netbox(read_write=True)
        self.netbox_server = self.netbox.get_server(self.args.host)

        # Shortcut variables
        self.host = self.args.host
        self.fqdn = self.netbox_server.fqdn
        self.mgmt_fqdn = self.netbox_server.mgmt_fqdn
        self.output_filename = self._get_output_filename(spicerack.username)
        self.actions = spicerack.actions
        self.host_actions = self.actions[self.host]
        self.confctl_services = []

        if self.netbox_server.virtual:
            raise RuntimeError(f'Host {self.host} is a virtual machine. VMs are not yet supported.')

        self.dns = spicerack.dns()
        self.icinga_host = spicerack.icinga_hosts([self.host])
        self.ipmi = spicerack.ipmi(self.mgmt_fqdn)
        self.reason = spicerack.admin_reason('Host reimage', task_id=self.args.task_id)
        self.puppet_master = spicerack.puppet_master()
        self.debmonitor = spicerack.debmonitor()
        self.confctl = spicerack.confctl('node')
        self.remote = spicerack.remote()
        self.remote_host = self.remote.query(self.fqdn)
        # The same as above but using the SSH key valid only during installation before the first Puppet run
        self.remote_installer = spicerack.remote(installer=True).query(self.fqdn)
        # Get a Puppet instance for the current cumin host to update the known hosts file
        remote_localhost = self.remote.query(f'{self.reason.hostname}.*')
        if len(remote_localhost) != 1:
            raise RuntimeError(f'Localhost matched the wrong number of hosts ({len(remote_localhost)}) for '
                               f'query "{self.reason.hostname}.*": {remote_localhost}')
        self.puppet_localhost = spicerack.puppet(remote_localhost)
        self.puppet = spicerack.puppet(self.remote_host)
        # The same as above but using the SSH key valid only during installation before the first Puppet run
        self.puppet_installer = spicerack.puppet(self.remote_installer)
        self.downtime = Downtime(spicerack)

        self._validate()

        # Keep track of some specific actions for the eventual rollback
        self.rollback_masks = False
        self.rollback_depool = False

        if self.args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.fqdn}'

    def rollback(self):
        """Update the Phabricator task with the failure."""
        if self.rollback_masks:
            self._unmask_units()
        if self.rollback_depool:
            self._repool()

        self.host_actions.failure('The reimage failed, see the cookbook logs for the details')
        if self.phabricator is not None:
            self.phabricator.task_comment(self.args.task_id, f'Cookbook {__name__} executed:\n{self.actions}\n')

    def _get_output_filename(self, username):
        """Return the absolute path of the file to use for the cumin output."""
        start = datetime.utcnow().strftime('%Y%m%d%H%M')
        pid = os.getpid()
        host = self.host.replace('.', '_')
        return f'/var/log/wmf-auto-reimage/{start}_{username}_{pid}_{host}.out'

    def _validate(self):
        """Perform all pre-reimage validation steps."""
        for dns_name in (self.fqdn, self.mgmt_fqdn):
            self.dns.resolve_ips(dns_name)  # Will raise if not valid

        self.ipmi.check_connection()  # Will raise if unable to connect

        # Validate that the host has a signed Puppet certificate
        if not self.args.new:
            try:
                self.puppet_master.verify(self.fqdn)
            except PuppetMasterError:
                if self.args.no_verify:
                    logger.warning('No valid certificate for %s, but --no-verify is set.', self.fqdn)
                else:
                    raise

    def _depool(self):
        """Depool all the pooled services for the host."""
        logger.info('Depooling services')
        self.confctl_services = list(self.confctl.filter_objects({'pooled': 'yes'}, name=self.fqdn))
        self.confctl.update_objects({'pooled': False}, self.confctl_services)
        self.rollback_depool = True
        updated = '\n'.join(service.tags for service in self.confctl_services)
        self.host_actions.success(f'Depooled the following services from confctl:\n{updated}')
        logger.info('Waiting for 3 minutes to allow for any in-flight connection to complete')
        time.sleep(180)

    def _repool(self):
        """Remind the user that the services were not repooled automatically."""
        if not self.confctl_services:
            return

        services = '\n'.join(service.tags for service in self.confctl_services)
        self.host_actions.warning(f'The following services were pooled before the reimage. '
                                  f'The repool is currently left to the user:\n{services}')

    def _install_os(self):
        """Perform the OS reinstall."""
        self.debmonitor.host_delete(self.fqdn)
        self.host_actions.success('Removed from Debmonitor')
        pxe_reboot_time = datetime.utcnow()
        self.ipmi.force_pxe()
        self.host_actions.success('Forced PXE for next reboot')
        self.ipmi.reboot()
        self.host_actions.success('Host rebooted via IPMI')
        self.remote_installer.wait_reboot_since(pxe_reboot_time)
        self.host_actions.success('Host up (Debian installer)')
        time.sleep(30)  # Avoid race conditions, the host is in the d-i, need to wait anyway
        di_reboot_time = datetime.utcnow()
        self.remote_installer.wait_reboot_since(di_reboot_time)
        self.host_actions.success('Host up (new fresh OS)')

    def _populate_puppetdb(self):
        """Run Puppet in noop mode to populate the exported resources in PuppetDB to downtime it on Icinga."""
        self.remote_installer.run_sync(Command('puppet agent -t --noop &> /dev/null', ok_codes=[]))
        self.host_actions.success('Run Puppet in NOOP mode to populate exported resources in PuppetDB')

        @retry(tries=10, backoff_mode='linear')
        def poll_puppetdb():
            """Poll PuppetDB until we find the Nagios_host resource for the newly installed host."""
            puppetdb_host = self.dns.resolve_ptr(self.dns.resolve_ipv4('puppetdb-api.discovery.wmnet')[0])[0]
            response = requests.post(f'https://{puppetdb_host}/pdb/query/v4/resources/Nagios_host/{self.host}')
            json_response = response.json()
            if not json_response:  # PuppetDB returns empty list for non-matching results
                raise SpicerackError(f'Nagios_host resource with title {self.host} not found yet')

            if len(json_response) != 1:
                raise RuntimeError(f'Expected 1 result from PuppetDB got {len(json_response)}')
            if json_response[0]['exported'] is not True:
                raise RuntimeError(
                    f'Expected the Nagios_host resource to be exported, got: {json_response[0]["exported"]}')

        poll_puppetdb()
        self.host_actions.success('Found Nagios_host resource for this host in PuppetDB')

    def _mask_units(self):
        """Mask systemd units."""
        if not self.args.mask:
            return

        commands = [f'systemctl mask {service}.service' for service in self.args.mask]
        self.remote_installer.run_sync(*commands)
        self.rollback_masks = True
        self.host_actions.success(f'Masked systemd units: {self.args.mask}')

    def _unmask_units(self):
        """Remind the user that masked services were not automatically unmasked."""
        if not self.args.mask:
            return

        commands = '\n'.join(f'systemctl unmask {service}.service\n' for service in self.args.mask)
        self.host_actions.warning(f'The masked units are not automatically unmasked. To unmask them run:\n{commands}')

    def _httpbb(self):
        """Run the httpbb tests."""
        if not self.args.httpbb:
            return

        command = f'httpbb /srv/deployment/httpbb-tests/appserver/* --host={self.fqdn}'
        deployment_host = self.remote.query(self.dns.resolve_cname('deployment.eqiad.wmnet'))
        try:
            deployment_host.run_sync(command, timeout=120)
            self.host_actions.success('Run of httpbb tests was successful')
        except RemoteExecutionError:
            # We don't want to fail upon this failure, this is just a validation test for the user.
            self.host_actions.warning('Failed to run httpbb tests')

    def run(self):
        """Execute the reimage."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                f'Cookbook {__name__} was started by {self.reason.owner} for host {self.fqdn}')

        if not self.args.new:
            if not self.args.no_downtime:
                confirm_on_failure(self.icinga_host.downtime, self.reason)
                self.host_actions.success('Downtimed on Icinga')

            if self.args.conftool:
                self._depool()

            try:
                self.puppet.disable(self.reason)
                self.host_actions.success('Disabled Puppet')
            except RemoteExecutionError:
                self.host_actions.warning('Unabled to disable Puppet, the host may have been unreachable')

            self.puppet_master.destroy(self.fqdn)
            self.host_actions.success('Removed from Puppet and PuppetDB')

        if self.args.no_pxe:
            logger.info('Skipping PXE reboot and associated steps as --no-pxe is set. Assuming new OS is in place.')
        else:
            self._install_os()

        self._mask_units()
        fingerprint = self.puppet_installer.regenerate_certificate()[self.fqdn]
        self.host_actions.success('Generated Puppet certificate')
        self.puppet_master.wait_for_csr(self.fqdn)
        self.puppet_master.sign(self.fqdn, fingerprint)
        self.host_actions.success('Signed new Puppet certificate')

        self._populate_puppetdb()
        downtime_args = ['--force-puppet', '--reason', 'host reimage', '--hours', '2', self.fqdn]
        self.downtime.get_runner(self.downtime.argument_parser().parse_args(downtime_args)).run()
        self.host_actions.success('Downtimed the new host on Icinga')

        puppet_first_run = confirm_on_failure(self.puppet_installer.first_run)
        self.host_actions.success('First Puppet run')
        with open(self.output_filename, 'w', encoding='utf8') as output_file:
            for _, output in puppet_first_run:
                output_file.write(output.message().decode())

        self.ipmi.check_bootparams()
        self.host_actions.success('Checked BIOS boot parameters are back to normal')

        # Run puppet locally to get the new host public keys
        self.puppet_localhost.run(quiet=True)

        reboot_time = datetime.utcnow()
        self.remote_host.reboot()
        self.remote_host.wait_reboot_since(reboot_time)
        self.host_actions.success('Rebooted')
        self.puppet.wait_since(reboot_time)
        self.host_actions.success('Automatic Puppet run was successful')

        self._httpbb()
        self._unmask_units()
        self._repool()

        # Comment on the Phabricator task
        if self.phabricator is not None:
            self.phabricator.task_comment(self.args.task_id, f'Cookbook {__name__} completed:\n{self.actions}\n')

        if self.host_actions.has_failures:
            return 1

        return 0
