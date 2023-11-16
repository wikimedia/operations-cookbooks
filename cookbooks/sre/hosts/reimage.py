"""Image or re-image a physical host."""
import ipaddress
import logging
import os
import re
import time

from datetime import datetime
from pathlib import Path
from textwrap import dedent
from typing import Union

from requests.exceptions import RequestException

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.dhcp import DHCPConfMac, DHCPConfOpt82
from spicerack.exceptions import SpicerackError
from spicerack.ganeti import Ganeti, GanetiRAPI, GntInstance
from spicerack.icinga import IcingaError
from spicerack.ipmi import Ipmi
from spicerack.puppet import PuppetMaster, PuppetServer
from spicerack.remote import RemoteError, RemoteExecutionError
from wmflib.interactive import AbortError, ask_confirmation, ask_input, confirm_on_failure, ensure_shell_is_durable

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import OS_VERSIONS
from cookbooks.sre.puppet import get_puppet_version

logger = logging.getLogger(__name__)


class Reimage(CookbookBase):
    """Image or re-image a physical host or a ganeti VM

    All data will be lost unless a specific partman recipe to retain partition data is used.

    Usage:
        cookbook sre.hosts.reimage --os bullseye -t T12345 example1001
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
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
        parser.add_argument(
            '--no-check-icinga', action='store_true',
            help='Do not wait for optimal status in Icinga after the reimage and do not remove the Icinga downtime.')
        parser.add_argument(
            '--pxe-media', default='installer',
            help=('Specify a different media suffix to use in the PXE settings of the DHCP configuration. To be used '
                  'when a specific installer is needed that is available as tftpboot/$OS-$PXE_MEDIA/.'))
        parser.add_argument('-t', '--task-id', help='the Phabricator task ID to update and refer (i.e.: T12345)')
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. Mandatory parameter. One of %(choices)s.')
        parser.add_argument('-p', '--puppet-version', choices=(5, 7), type=int,
                            help='The puppet version to use when reimaging. One of %(choices)s.')
        parser.add_argument('host', help='Short hostname of the host to be reimaged, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ReimageRunner(args, self.spicerack)


class ReimageRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements
        """Initiliaze the reimage runner."""
        ensure_shell_is_durable()
        self.args = args
        self.host = self.args.host

        if '.' in self.host:
            raise RuntimeError('You need to pass only the host name, not the FQDN.')

        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.host, read_write=True)
        self.netbox_data = self.netbox_server.as_dict()

        ask_confirmation(f'ATTENTION: destructive action for host: {self.host}\nAre you sure to proceed?')

        # Shortcut variables
        self.fqdn = self.netbox_server.fqdn
        self.output_filename = self._get_output_filename(spicerack.username)
        self.actions = spicerack.actions
        self.host_actions = self.actions[self.host]
        self.confctl_services = []
        self.dns = spicerack.dns()
        self.icinga_host = spicerack.icinga_hosts([self.host])
        self.alerting_host = spicerack.alerting_hosts([self.host])
        self.alertmanager_host = spicerack.alertmanager_hosts([self.host])
        self.ganeti: Ganeti = spicerack.ganeti()
        self.reason = spicerack.admin_reason('Host reimage', task_id=self.args.task_id)
        self.debmonitor = spicerack.debmonitor()
        self.confctl = spicerack.confctl('node')
        self.remote = spicerack.remote()
        self.spicerack: Spicerack = spicerack
        self.requests = spicerack.requests_session(__name__, timeout=(5.0, 30.0))
        self.virtual: bool = self.netbox_server.virtual

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

        if args.puppet_version is None and args.new:
            args.puppet_version = int(ask_input("Select puppet version to install with", ('5', '7')))

        if args.puppet_version == 7 and args.os == 'buster':
            raise RuntimeError('Puppet 7 is not supported on buster you must first upgrade the os.')

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
        self.puppet_configmaster = spicerack.puppet(self.remote.query('O:config_master'))
        self.puppet_server = self._get_puppet_server()

        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])

        # Keep track of some specific actions for the eventual rollback
        self.rollback_masks = False
        self.rollback_depool = False
        self.rollback_clear_dhcp_cache = False

        if self.args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

        # Load properties / attributes specific to either virtual
        # or physical hosts.
        self.dhcp_config: Union[DHCPConfMac, DHCPConfOpt82]
        if self.virtual:
            self.ganeti_instance: GntInstance = self.ganeti.instance(self.fqdn)
            self.ganeti_rapi: GanetiRAPI = self.ganeti.rapi(self.ganeti_instance.cluster)
            self.ganeti_data: dict = self.ganeti_rapi.fetch_instance(self.fqdn)
            self.dhcp_config = self._get_dhcp_config_mac()
        else:
            self.mgmt_fqdn = self.netbox_server.mgmt_fqdn
            self.ipmi: Ipmi = self.spicerack.ipmi(self.mgmt_fqdn)
            self.dhcp_config = self._get_dhcp_config_opt82()

        self._validate()

    def _get_puppet_server(self) -> Union[PuppetMaster, PuppetServer]:
        """Validate that the puppet version is set correctly."""
        if self.args.new and self.args.puppet_version == 7:
            ask_confirmation(dedent(
                f"""\
                Please add the following hiera entry to:

                hieradata/hosts/{self.host}.yaml
                    profile::puppet::agent::force_puppet7: true
                    acmechief_host: acmechief2002.codfw.wmnet

                Press continue when the change is merged
                """
            ))

        if not self.args.new:
            current_puppet_version = get_puppet_version(self.requests, self.host)
            if current_puppet_version is None:
                raise RuntimeError(f"unable to get puppet version for {self.host}")
            if self.args.puppet_version is None:
                self.args.puppet_version = current_puppet_version.major
            else:
                if self.args.puppet_version == 5 and current_puppet_version.major == 7:
                    raise RuntimeError("This cookbook does not support going from puppet 5 to puppet 7")
                if self.args.puppet_version != current_puppet_version.major:
                    ask_confirmation(f"you have specified puppet version {self.args.puppet_version} however {self.host}"
                                     f" is currently running puppet version {current_puppet_version}."
                                     " Are you sure you want to continue")
                if self.args.puppet_version == 7 and current_puppet_version.major != 7:
                    # Lets migrate the host first
                    ret = self.spicerack.run_cookbook("sre.puppet.migrate-host", [self.fqdn])
                    if ret:
                        raise RuntimeError(f"Failed to run: sre.puppet.migrate-host {self.fqdn}")

        if self.args.puppet_version == 5:
            return self.spicerack.puppet_master()

        return self.spicerack.puppet_server()

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.fqdn} with OS {self.args.os}'

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=self.host, concurrency=1, ttl=3600)

    def rollback(self):
        """Update the Phabricator task with the failure."""
        if self.rollback_masks:
            self._unmask_units()
        if self.rollback_depool:
            self._repool()
        if self.rollback_clear_dhcp_cache:
            self._clear_dhcp_cache()

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
        dns_names = (self.fqdn,) if self.virtual else (self.fqdn, self.mgmt_fqdn)
        for dns_name in dns_names:
            self.dns.resolve_ips(dns_name)  # Will raise if not valid

        if not self.virtual:
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
            tags = [f'name={re.escape(self.fqdn)}']
            tags += [f'{k}={re.escape(v)}' for k, v in obj.tags.items()]
            tags_line = ','.join(tags)
            commands.append(f"sudo confctl select '{tags_line}' set/pooled={obj.pooled}")
            if obj.weight <= 0:
                weights.append("sudo confctl select '{tags_line}' set/weight=NN")

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

    def _get_dhcp_config_mac(self) -> DHCPConfMac:
        ip = ipaddress.IPv4Interface(self.netbox_data['primary_ip4']['address']).ip
        mac = self.ganeti_data.get('nic.macs', None)

        if not isinstance(ip, ipaddress.IPv4Address):
            raise RuntimeError(f'Unable to find primary IPv4 address for {self.host}.')

        if not mac or len(mac) != 1:
            raise RuntimeError(f'Unable to get MAC from Ganeti for {self.host}')

        return DHCPConfMac(
            hostname=self.host,
            ipv4=ip,
            mac=mac[0],
            ttys=0,
            distro=self.args.os
        )

    def _get_dhcp_config_opt82(self) -> DHCPConfOpt82:
        """Instantiate a DHCP configuration to be used for the reimage."""
        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        netbox_iface = netbox_host.primary_ip.assigned_object
        switch_iface = netbox_iface.connected_endpoint
        if netbox_iface.type.value == 'bridge':
            # We need to get the physical port that belongs to the bridge instead
            bridge_members = self.netbox.api.dcim.interfaces.filter(device=netbox_host.name, bridge_id=netbox_iface.id)
            connected_ifaces = [iface for iface in bridge_members if iface.connected_endpoint is not None]
            if len(connected_ifaces) == 1:
                switch_iface = connected_ifaces[0].connected_endpoint
        if switch_iface is None:
            raise RuntimeError(f'Error finding primary interface connected switch port for {self.host}. Netbox '
                               'model of server connections is invalid.')

        switch_hostname = (
            switch_iface.device.virtual_chassis.name.split('.')[0]
            if switch_iface.device.virtual_chassis is not None
            else switch_iface.device.name
        )

        return DHCPConfOpt82(
            hostname=self.host,
            ipv4=ipaddress.IPv4Interface(netbox_host.primary_ip4).ip,
            switch_hostname=switch_hostname,
            switch_iface=f'{switch_iface}.0',  # In Netbox we have just the main interface
            vlan=switch_iface.untagged_vlan.name,
            ttys=1,
            distro=self.args.os,
            media_type=self.args.pxe_media,
        )

    def _install_os(self):
        """Perform the OS reinstall."""
        pxe_reboot_time = datetime.utcnow()

        if self.virtual:
            # Prepare a Ganeti VM for reboot and PXE boot for
            # reimaging.
            self.ganeti_instance.set_boot_media('network')
            self.host_actions.success('Forced PXE for next reboot')
            self.ganeti_instance.shutdown()
            self.ganeti_instance.startup()
            self.host_actions.success('Host rebooted via gnt-instance')
        else:
            # Prepare a physical host for reboot and PXE boot
            # for reimaging.
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
                             f'with: sudo install-console {self.fqdn}')
        self.host_actions.success('Host up (Debian installer)')

        puppet_version_cmd = f"printf {self.args.puppet_version} > /tmp/puppet_version"
        try:
            self.remote_installer.run_sync(puppet_version_cmd, print_output=False, print_progress_bars=False)
        except RemoteExecutionError:
            ask_confirmation('Unable to set the puppet version inside the Debian installer, please do manually '
                             f'with: sudo install-console {self.fqdn}\n{puppet_version_cmd}')
        self.host_actions.success('Add puppet_version metadata to Debian installer')

        # Reset boot media allowing the newly installed OS to boot.
        if self.virtual:
            self.ganeti_instance.set_boot_media('disk')
            self.host_actions.success('Set boot media to disk')
        else:
            self.ipmi.remove_boot_override()
            self.ipmi.check_bootparams()
            self.host_actions.success('Checked BIOS boot parameters are back to normal')

        self.rollback_clear_dhcp_cache = True
        self.remote_installer.wait_reboot_since(di_reboot_time, print_progress_bars=False)
        try:
            self.remote_installer.run_sync(f'! {env_command}', print_output=False, print_progress_bars=False)
        except RemoteExecutionError:
            ask_confirmation('Unable to verify that the host rebooted into the new OS, it might still be in the '
                             f'Debian installer, please verify manually with: sudo install-console {self.fqdn}')

        result = self.remote_installer.run_sync('lsb_release -sc', print_output=False, print_progress_bars=False)

        distro: str = 'unknown'
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
            query = {
                "query": [
                    "and",
                    ["=", "title", self.host],
                    ["=", "type", "Nagios_host"],
                    ["=", "exported", True]
                ]
            }
            response = self.requests.post(
                'https://puppetdb-api.discovery.wmnet:8090/pdb/query/v4/resources',
                json=query
            )
            json_response = response.json()
            if not json_response:  # PuppetDB returns empty list for non-matching results
                raise SpicerackError(f'Nagios_host resource with title {self.host} not found yet')

            if len(json_response) != 1:
                raise RuntimeError(f'Expected 1 result from PuppetDB got {len(json_response)}')

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
        if self.args.no_check_icinga:
            self.host_actions.warning('//Skipping waiting for Icinga optimal status and not removing the downtime, '
                                      '--no-check-icinga was set//')
            return

        try:
            self.icinga_host.wait_for_optimal()
            self.host_actions.success('Icinga status is optimal')
            self.icinga_host.remove_downtime()
            self.host_actions.success('Icinga downtime removed')
        except IcingaError:  # Do not fail here, just report it to the user, not all hosts are optimal upon reimage
            self.host_actions.warning('//Icinga status is not optimal, downtime not removed//')

    def _clear_dhcp_cache(self):
        """If the host is connected to EVPN switch clear the DHCP and MAC caches. Workaround for T306421."""
        if self.virtual:
            return
        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        # We only have EVPN running in eqiad and codfw
        if netbox_host.site.slug not in ('eqiad', 'codfw'):
            return
        iface = netbox_host.primary_ip.assigned_object
        # Return if connected switch is not QFX5120 - all EVPN ones are
        if not iface.connected_endpoint.device.device_type.model.lower().startswith('qfx5120'):
            return
        # Otherwise check if a vlan with the rack name exists
        rack_vlan_name = f'private1-{netbox_host.rack.name.lower()}-{netbox_host.site.slug}'
        netbox_rack_vlan = self.netbox.api.ipam.vlans.get(name=rack_vlan_name)
        if netbox_rack_vlan is None:
            return

        # Get switch IP and server interface MAC to clear caches
        switch_fqdn = iface.connected_endpoint.device.primary_ip.dns_name
        switch = self.remote.query(f'D{{{switch_fqdn}}}')
        ip = ipaddress.ip_interface(netbox_host.primary_ip4).ip
        mac_command = '/usr/bin/facter -p networking.mac'
        result = self.remote_host.run_sync(mac_command, is_safe=True, print_progress_bars=False, print_output=False)

        mac: str = ''
        for _, output in result:
            mac = output.message().decode().strip()

        commands = [
            f'clear dhcp relay binding {ip} routing-instance PRODUCTION',
            f'clear ethernet-switching mac-ip-table {mac}'
        ]
        switch.run_sync(*commands, print_progress_bars=False)
        self.host_actions.success('Cleared switch DHCP cache and MAC table for the host IP and MAC (EVPN Switch)')

    def _update_netbox_data(self):
        """Update Netbox data from PuppetDB running the Netbox script."""
        # Apparently pynetbox doesn't allow to execute a Netbox script
        url = self.netbox.api.extras.scripts.get('import_server_facts.ImportPuppetDB').url
        headers = {'Authorization': f'Token {self.netbox.api.token}'}
        data = {'data': {'device': self.host}, 'commit': 1}

        @retry(tries=30, backoff_mode='constant', exceptions=(ValueError, RequestException))
        def _poll_netbox_job(url):
            """Poll Netbox to get the result of the script run."""
            result = self.requests.get(url, headers=headers)
            result.raise_for_status()
            data = result.json()['data']
            if data is None:
                raise ValueError(f'No data from job result {url}')

            for line in data['log']:
                logger.info('[%s] %s', line['status'], line['message'])

        result = None
        try:
            result = self.requests.post(url, headers=headers, json=data)
            result.raise_for_status()
            self.host_actions.success('Updated Netbox data from PuppetDB')
        except RequestException:
            self.host_actions.failure(f'**Failed to execute Netbox script, try manually**: {url}')
            if result:
                logger.error(result.text)
        else:
            job_url = result.json()['result']['url']
            try:
                _poll_netbox_job(job_url)
            except (ValueError, RequestException) as e:
                logger.error(e)
                self.host_actions.failure(f'**Failed to get Netbox script results, try manually**: {job_url}')

    def run(self):  # pylint: disable=too-many-statements,too-many-branches
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
            if not self.args.no_pxe:
                try:
                    self.puppet.disable(self.reason)
                    self.host_actions.success('Disabled Puppet')
                except RemoteExecutionError:
                    self.host_actions.warning('//Unable to disable Puppet, the host may have been unreachable//')

        self.puppet_server.delete(self.fqdn)
        if self.args.puppet_version == 7:  # Ensure we delete the old certificate from the Puppet 5 infra
            self.spicerack.puppet_master().delete(self.fqdn)

        self.host_actions.success('Removed from Puppet and PuppetDB if present and deleted any certificates')
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
        self.puppet_server.wait_for_csr(self.fqdn)
        self.puppet_server.sign(self.fqdn, fingerprint)
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
            output_filename = self._get_output_filename(self.spicerack.username)
            results = None
            try:
                results = self.puppet_installer.first_run()
                self.host_actions.success(f'First Puppet run completed and logged in {output_filename}')
                return True
            except RemoteExecutionError as e:
                results = e.results
                logger.error(('First Puppet run failed:\n'
                              'Check the logs in %s and at https://puppetboard.wikimedia.org/node/%s\n'
                              'Inspect the host with: sudo install-console %s'), output_filename, self.fqdn, self.fqdn)
                self.host_actions.warning(f'//First Puppet run failed and logged in {output_filename}, asking the '
                                          'operator what to do//')
                raise
            finally:
                if results is not None:
                    with open(output_filename, 'w', encoding='utf8') as output_file:
                        for _, output in results:
                            output_file.write(output.message().decode())

        try:
            first_puppet_run = confirm_on_failure(_first_puppet_run)
        except AbortError:
            self.host_actions.failure('**First Puppet run failed and the operator aborted**')
            raise

        if first_puppet_run is None:
            self.host_actions.warning('//First Puppet run failed and the operator skipped it//')

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
        current_status = self.netbox_server.status
        if not self.virtual and current_status in ('planned', 'failed'):
            self.netbox_server.status = 'active'
            self.host_actions.success(f'Updated Netbox status {current_status} -> active')
            hiera_ret = self.spicerack.run_cookbook(
                'sre.puppet.sync-netbox-hiera', [f'Triggered by {__name__}: {self.reason.reason}'])
            if hiera_ret:
                hiera_message = 'Failed to run the sre.puppet.sync-netbox-hiera cookbook, run it manually'
                logger.warning(hiera_message)
                self.host_actions.warning(f'//{hiera_message}//')
            else:
                self.host_actions.success('The sre.puppet.sync-netbox-hiera cookbook was run successfully')

        # See T306421
        self._clear_dhcp_cache()

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
