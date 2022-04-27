"""Image or re-image a physical host."""
import argparse
import ipaddress
import logging
import os
import time

from datetime import datetime
from pathlib import Path

import requests

from cumin.transports import Command
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.dhcp import DHCPConfOpt82
from spicerack.exceptions import SpicerackError
from spicerack.icinga import IcingaError
from spicerack.remote import RemoteError, RemoteExecutionError
from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import OS_VERSIONS


logger = logging.getLogger(__name__)


class Reimage(CookbookBase):
    """Image or re-image a physical host.

    All data will be lost unless a specific partman recipe to retain partition data is used.

    Usage:
        cookbook sre.hosts.reimage -t T12345 example1001
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            '--no-downtime', action='store_true',
            help=('do not set the host in downtime on Icinga/Alertmanager before the reimage. Included if --new is '
                  'set. The host will be downtimed after the reimage in any case.'))
        parser.add_argument(
            '--no-pxe', action='store_true',
            help=('do not reboot into PXE and reimage. To be used when the reimage had issues and was manually fixed '
                  'after the timeout hence the run failed.'))
        parser.add_argument(
            '--new', action='store_true',
            help=('for first imaging of new hosts that are not in yet in Puppet and this is their first '
                  'imaging. Skips some steps prior to the reimage.'))
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
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. One of %(choices)s')
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
        self.host = self.args.host

        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.host, read_write=True)
        self.netbox_data = self.netbox_server.as_dict()

        ask_confirmation(f'ATTENTION: destructive action for host: {self.host}\nAre you sure to proceed?')

        # Shortcut variables
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
        self.alerting_host = spicerack.alerting_hosts([self.host])
        self.alertmanager_host = spicerack.alertmanager_hosts([self.host])
        self.ipmi = spicerack.ipmi(self.mgmt_fqdn)
        self.reason = spicerack.admin_reason('Host reimage', task_id=self.args.task_id)
        self.puppet_master = spicerack.puppet_master()
        self.debmonitor = spicerack.debmonitor()
        self.confctl = spicerack.confctl('node')
        self.remote = spicerack.remote()
        self.spicerack = spicerack

        try:
            self.remote_host = self.remote.query(self.fqdn)
            if self.args.new:
                ask_confirmation(f'Host {self.fqdn} was found in PuppetDB but --new was set. Are you sure you want to '
                                 'proceed? The --new option will be unset')
                self.args.new = False  # Unset --new
                logger.info('The option --new has been unset')
        except RemoteError as e:
            self.remote_host = self.remote.query(f'D{{{self.fqdn}}}')  # Use the Direct backend instead
            if not self.args.new:
                raise RuntimeError(f'Host {self.fqdn} was not found in PuppetDB but --new was not set. Check that the '
                                   'FQDN is correct. If the host is new or has disappeared from PuppetDB because down '
                                   'for too long use --new.') from e

        if len(self.remote_host) != 1:
            raise RuntimeError(
                f'Expected 1 host for query {self.fqdn} but got {len(self.remote_host)}: {self.remote_host}')

        # The same as self.remote_host but using the SSH key valid only during installation before the first Puppet run
        self.remote_installer = spicerack.remote(installer=True).query(self.fqdn)
        # Get a Puppet instance for the current cumin host to update the known hosts file
        remote_localhost = self.remote.query(f'{self.reason.hostname}.*')
        if len(remote_localhost) != 1:
            raise RuntimeError(f'Localhost matched the wrong number of hosts ({len(remote_localhost)}) for '
                               f'query "{self.reason.hostname}.*": {remote_localhost}')
        self.puppet_localhost = spicerack.puppet(remote_localhost)
        self.puppet = spicerack.puppet(self.remote_host)
        # The same as self.puppet but using the SSH key valid only during installation before the first Puppet run
        self.puppet_installer = spicerack.puppet(self.remote_installer)
        self.puppet_configmaster = spicerack.puppet(self.remote.query('P:configmaster'))

        # DHCP automation
        try:
            self.dhcp_hosts = self.remote.query(f'A:installserver-light and A:{self.netbox_data["site"]["slug"]}')
        except RemoteError:  # Fallback to eqiad's install server if the above fails, i.e. for a new DC
            self.dhcp_hosts = self.remote.query('A:installserver-light and A:eqiad')
        self.dhcp = spicerack.dhcp(self.dhcp_hosts)
        self.dhcp_config = self._get_dhcp_config()

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
        return f'for host {self.fqdn} with OS {self.args.os}'

    def rollback(self):
        """Update the Phabricator task with the failure."""
        if self.rollback_masks:
            self._unmask_units()
        if self.rollback_depool:
            self._repool()

        self.host_actions.failure('**The reimage failed, see the cookbook logs for the details**')
        logger.error('Reimage executed with errors:\n%s\n', self.actions)
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                (f'Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} executed with errors:'
                 f'\n{self.actions}\n'),
            )

    def _get_output_filename(self, username):
        """Return the absolute path of the file to use for the cumin output."""
        start = datetime.utcnow().strftime('%Y%m%d%H%M')
        pid = os.getpid()
        host = self.host.replace('.', '_')
        base_dir = Path('/var/log/spicerack/sre/hosts/reimage/')
        base_dir.mkdir(parents=True, exist_ok=True)

        return base_dir / f'{start}_{username}_{pid}_{host}.out'

    def _validate(self):
        """Perform all pre-reimage validation steps."""
        for dns_name in (self.fqdn, self.mgmt_fqdn):
            self.dns.resolve_ips(dns_name)  # Will raise if not valid

        self.ipmi.check_connection()  # Will raise if unable to connect

    def _depool(self):
        """Depool all the pooled services for the host."""
        if not self.args.conftool:
            return

        logger.info('Depooling services')
        self.confctl_services = list(self.confctl.filter_objects({}, name=self.fqdn))  # Get a copy for later usage
        confctl_services = self.confctl.filter_objects({}, name=self.fqdn)  # Use this copy for the update
        if not self.confctl_services:
            raise RuntimeError(f'-c/--conftool was set but no objects were found on confctl for name={self.fqdn}')

        self.confctl.update_objects({'pooled': self.args.conftool_value}, confctl_services)
        self.rollback_depool = True
        services_lines = '\n'.join(str(service) for service in self.confctl_services)
        self.host_actions.success(
            f'Set pooled={self.args.conftool_value} for the following services on confctl:\n{services_lines}')
        logger.info('Waiting for 3 minutes to allow for any in-flight connection to complete')
        time.sleep(180)

    def _repool(self):
        """Remind the user that the services were not repooled automatically."""
        if not self.confctl_services:
            return

        commands = []
        weights = []
        for obj in self.confctl_services:
            if obj.pooled == self.args.conftool_value:
                continue  # Nothing to do
            tags = ','.join(f'{k}={v}' for k, v in obj.tags.items())
            commands.append(f"sudo confctl select '{tags}' set/pooled={obj.pooled}")
            if obj.weight <= 0:
                weights.append("sudo confctl select '{tags}' set/weight=NN")

        if weights:
            weights_lines = '\n'.join(weights)
            self.host_actions.warning(
                f'//Some services have a zero weight, you have to set a weight with//:\n{weights_lines}')

        if commands:
            commands_lines = '\n'.join(commands)
            self.host_actions.warning('//Services in confctl are not automatically pooled, to restore the previous '
                                      f'state you have to run the following commands://\n{commands_lines}')
        else:
            self.host_actions.success('No changes in confctl are needed to restore the previous state.')

    def _get_dhcp_config(self):
        """Instantiate a DHCP configuration to be used for the reimage."""
        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        switch_iface = netbox_host.primary_ip.assigned_object.connected_endpoint
        if switch_iface is None:  # Temporary workaround to support Ganeti hosts
            ifaces = self.netbox.api.dcim.interfaces.filter(device=netbox_host.name, mgmt_only=False)
            connected_ifaces = [iface for iface in ifaces if iface.connected_endpoint is not None]
            if len(connected_ifaces) == 1:
                switch_iface = connected_ifaces[0].connected_endpoint
            else:
                raise RuntimeError(f'Unable to find the switch interface to which {self.host} is connected to. The '
                                   f'interfaces that are connected in Netbox are: {connected_ifaces}')

        switch_hostname = (
            switch_iface.device.virtual_chassis.name.split('.')[0]
            if switch_iface.device.virtual_chassis is not None
            else switch_iface.device.name
        )

        return DHCPConfOpt82(
            hostname=self.host,
            ipv4=ipaddress.ip_interface(netbox_host.primary_ip4).ip,
            switch_hostname=switch_hostname,
            switch_iface=f'{switch_iface}.0',  # In Netbox we have just the main interface
            vlan=switch_iface.untagged_vlan.name,
            ttys=1,
            distro=self.args.os,
        )

    def _install_os(self):
        """Perform the OS reinstall."""
        pxe_reboot_time = datetime.utcnow()
        self.ipmi.force_pxe()
        self.host_actions.success('Forced PXE for next reboot')
        self.ipmi.reboot()
        self.host_actions.success('Host rebooted via IPMI')
        self.remote_installer.wait_reboot_since(pxe_reboot_time, print_progress_bars=False)
        time.sleep(30)  # Avoid race conditions, the host is in the d-i, need to wait anyway
        di_reboot_time = datetime.utcnow()
        env_command = 'grep -q "BOOT_IMAGE=debian-installer" /proc/cmdline'
        try:
            self.remote_installer.run_sync(env_command, print_output=False, print_progress_bars=False)
        except RemoteExecutionError:
            ask_confirmation('Unable to verify that the host is inside the Debian installer, please verify manually '
                             f'with: sudo install_console {self.fqdn}')

        self.host_actions.success('Host up (Debian installer)')
        self.remote_installer.wait_reboot_since(di_reboot_time, print_progress_bars=False)
        try:
            self.remote_installer.run_sync(f'! {env_command}', print_output=False, print_progress_bars=False)
        except RemoteExecutionError:
            ask_confirmation('Unable to verify that the host rebooted into the new OS, it might still be into the '
                             f'Debian installer, please verify manually with: sudo install_console {self.fqdn}')

        result = self.remote_installer.run_sync('lsb_release -sc', print_output=False, print_progress_bars=False)
        for _, output in result:
            distro = output.message().decode()

        if distro != self.args.os:
            message = f'New OS is {distro} but {self.args.os} was requested'
            self.host_actions.failure(message)
            raise RuntimeError(message)

        self.host_actions.success(f'Host up (new fresh {distro} OS)')

    def _populate_puppetdb(self):
        """Run Puppet in noop mode to populate the exported resources in PuppetDB to downtime it on Icinga."""
        self.remote_installer.run_sync(Command('puppet agent -t --noop &> /dev/null', ok_codes=[]),
                                       print_progress_bars=False)
        self.host_actions.success('Run Puppet in NOOP mode to populate exported resources in PuppetDB')

        @retry(tries=50, backoff_mode='linear')
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
        self.remote_installer.run_sync(*commands, print_progress_bars=False)
        self.rollback_masks = True
        self.host_actions.success(f'Masked systemd units: {self.args.mask}')

    def _unmask_units(self):
        """Remind the user that masked services were not automatically unmasked."""
        if not self.args.mask:
            return

        commands = '\n'.join(f'systemctl unmask {service}.service\n' for service in self.args.mask)
        self.host_actions.warning('//The masked units might not have been automatically unmasked by Puppet. '
                                  f'To unmask them run://\n{commands}')

    def _httpbb(self):
        """Run the httpbb tests."""
        if not self.args.httpbb:
            return

        command = Command(f'httpbb /srv/deployment/httpbb-tests/appserver/* --host={self.fqdn}', timeout=120)
        deployment_host = self.remote.query(self.dns.resolve_cname('deployment.eqiad.wmnet'))
        logger.info('Running httpbb tests')
        try:
            deployment_host.run_sync(command, print_progress_bars=False)
            self.host_actions.success('Run of httpbb tests was successful')
        except RemoteExecutionError:
            # We don't want to fail upon this failure, this is just a validation test for the user.
            self.host_actions.warning('//Failed to run httpbb tests//')

    def _check_icinga(self):
        """Best effort attempt to wait for Icinga to be optimal, do not fail if not."""
        self.icinga_host.recheck_all_services()
        self.host_actions.success('Forced a re-check of all Icinga services for the host')
        try:
            self.icinga_host.wait_for_optimal()
            self.host_actions.success('Icinga status is optimal')
            self.icinga_host.remove_downtime()
            self.host_actions.success('Icinga downtime removed')
        except IcingaError:  # Do not fail here, just report it to the user, not all hosts are optimal upon reimage
            self.host_actions.warning('//Icinga status is not optimal, downtime not removed//')

    def _update_netbox_data(self):
        """Update Netbox data from PuppetDB running the Netbox script."""
        # Apparently pynetbox doesn't allow to execute a Netbox script
        url = self.netbox.api.extras.scripts.get('interface_automation.ImportPuppetDB').url
        headers = {'Authorization': f'Token {self.netbox.api.token}'}
        data = {'data': {'device': self.host}, 'commit': 1}

        @retry(tries=10, backoff_mode='constant', exceptions=(ValueError, requests.exceptions.RequestException))
        def _poll_netbox_job(url):
            """Poll Netbox to get the result of the script run."""
            result = requests.get(url, headers=headers)
            result.raise_for_status()
            data = result.json()['data']
            if data is None:
                raise ValueError(f'No data from job result {url}')

            for line in data['log']:
                logger.info('[%s] %s', line['status'], line['message'])

        try:
            result = requests.post(url, headers=headers, json=data)
            result.raise_for_status()
            self.host_actions.success('Updated Netbox data from PuppetDB')
        except requests.exceptions.RequestException:
            self.host_actions.failure(f'**Failed to execute Netbox script, try manually**: {url}')
            logger.error(result.text)
        else:
            job_url = result.json()['result']['url']
            try:
                _poll_netbox_job(job_url)
            except (ValueError, requests.exceptions.RequestException) as e:
                logger.error(e)
                self.host_actions.failure(f'**Failed to get Netbox script results, try manually**: {job_url}')

    def run(self):  # pylint: disable=too-many-statements
        """Execute the reimage."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                f'Cookbook {__name__} was started by {self.reason.owner} {self.runtime_description}')

        downtime_id_pre_install = ''
        if not self.args.new:
            if not self.args.no_downtime:
                downtime_id_pre_install = confirm_on_failure(self.alerting_host.downtime, self.reason)
                self.host_actions.success('Downtimed on Icinga/Alertmanager')

            self._depool()
            try:
                self.puppet.disable(self.reason)
                self.host_actions.success('Disabled Puppet')
            except RemoteExecutionError:
                self.host_actions.warning('//Unable to disable Puppet, the host may have been unreachable//')

        self.puppet_master.delete(self.fqdn)
        self.host_actions.success('Removed from Puppet and PuppetDB if present')
        self.puppet_master.destroy(self.fqdn)
        self.host_actions.success('Deleted any existing Puppet certificate')
        self.debmonitor.host_delete(self.fqdn)
        self.host_actions.success('Removed from Debmonitor if present')

        if self.args.no_pxe:
            logger.info('Skipping PXE reboot and associated steps as --no-pxe is set. Assuming new OS is in place.')
        else:
            with self.dhcp.config(self.dhcp_config):
                self._install_os()

        self._mask_units()
        fingerprint = self.puppet_installer.regenerate_certificate()[self.fqdn]
        self.host_actions.success('Generated Puppet certificate')
        self.puppet_master.wait_for_csr(self.fqdn)
        self.puppet_master.sign(self.fqdn, fingerprint)
        self.host_actions.success('Signed new Puppet certificate')

        self._populate_puppetdb()
        downtime_retcode = self.spicerack.run_cookbook(
            'sre.hosts.downtime', ['--force-puppet', '--reason', 'host reimage', '--hours', '2', self.fqdn])
        if downtime_retcode == 0:
            self.host_actions.success('Downtimed the new host on Icinga/Alertmanager')
        else:
            self.host_actions.warning('//Unable to downtime the new host on Icinga/Alertmanager, the '
                                      f'sre.hosts.downtime cookbook returned {downtime_retcode}//')

        if downtime_id_pre_install:
            self.alertmanager_host.remove_downtime(downtime_id_pre_install)
            self.host_actions.success('Removed previous downtime on Alertmanager (old OS)')

        def _first_puppet_run():
            """Print a nicer message on failure."""
            # TODO: remove once Cumin returns partial output on failure
            try:
                return self.puppet_installer.first_run()
            except RemoteExecutionError:
                logger.error(('First Puppet run failed:\n'
                              'Check the logs at https://puppetboard.wikimedia.org/node/%s\n'
                              'Inspect the host with: sudo install_console %s'), self.fqdn, self.fqdn)
                self.host_actions.failure('**First Puppet run failed, asking the operator what to do**')
                raise

        puppet_first_run = confirm_on_failure(_first_puppet_run)
        self.host_actions.success(f'First Puppet run completed and logged in {self.output_filename}')
        with open(self.output_filename, 'w', encoding='utf8') as output_file:
            for _, output in puppet_first_run:
                output_file.write(output.message().decode())

        self.ipmi.remove_boot_override()
        self.ipmi.check_bootparams()
        self.host_actions.success('Checked BIOS boot parameters are back to normal')

        # Run puppet locally to get the new host public key, required to proceed
        self.puppet_localhost.run(quiet=True)
        # Run puppet on configmaster.wikimedia.org to allow wmf-update-known-hosts-production to get the new public
        # key and allow the user to SSH into the new host
        try:
            self.puppet_configmaster.run(quiet=True)
            self.host_actions.success('configmaster.wikimedia.org updated with the host new SSH public key for '
                                      'wmf-update-known-hosts-production')
        except RemoteExecutionError:
            self.host_actions.warning(f'//Unable to run puppet on {self.puppet_configmaster} to update '
                                      'configmaster.wikimedia.org with the new host SSH public key for '
                                      'wmf-update-known-hosts-production//')

        reboot_time = datetime.utcnow()
        self.remote_host.reboot()
        time.sleep(60)  # Temporary workaround to prevent a race condition
        self.remote_host.wait_reboot_since(reboot_time, print_progress_bars=False)
        self.host_actions.success('Rebooted')
        self.puppet.wait_since(reboot_time)
        self.host_actions.success('Automatic Puppet run was successful')

        self._httpbb()
        self._unmask_units()
        self._check_icinga()
        self._repool()
        self._update_netbox_data()
        if self.netbox_server.status == 'planned':
            self.netbox_server.status = 'staged'
            self.host_actions.success('Updated Netbox status planned -> staged')

        # Comment on the Phabricator task
        logger.info('Reimage completed:\n%s\n', self.actions)
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                (f'Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:\n'
                 f'{self.actions}\n'),
            )

        if self.host_actions.has_failures:
            return 1

        return 0
