"""Image or re-image a physical host or a Ganeti VM."""
import ipaddress
import json
import logging
import os
import re
import time

from datetime import datetime
from pathlib import Path

from requests.exceptions import RequestException

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack.dhcp import DHCPConfiguration, DHCPConfMac, DHCPConfOpt82, DHCPConfUUID
from spicerack.exceptions import SpicerackError
from spicerack.ganeti import Ganeti, GanetiRAPI, GntInstance
from spicerack.icinga import IcingaError
from spicerack.ipmi import Ipmi
from spicerack.redfish import ChassisResetPolicy, RedfishError
from spicerack.remote import RemoteError, RemoteExecutionError, RemoteCheckError
from wmflib.interactive import AbortError, ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from cookbooks.sre.puppet import get_puppet_fact
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import (
    DELL_VENDOR_SLUG,
    OS_VERSIONS,
    SUPERMICRO_VENDOR_SLUG,
    LEGACY_VLANS
)

logger = logging.getLogger(__name__)


class Reimage(CookbookBase):
    """Image or re-image a physical host or a ganeti VM

    All data will be lost unless a specific partman recipe to retain partition data is used.

    Usage:
        cookbook sre.hosts.reimage --os bullseye -t T12345 example1001
    """

    owner_team = "Infrastructure Foundations"
    argument_task_required = False

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
        parser.add_argument(
            '--move-vlan', action='store_true',
            help='Call the sre.hosts.move-vlan cookbook to migrate the host to the new VLAN during the reimage.'
                 'See https://wikitech.wikimedia.org/wiki/Vlan_migration for further information.')
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. Mandatory parameter. One of %(choices)s.')
        parser.add_argument('host', help='Short hostname of the host to be reimaged, not FQDN')
        parser.add_argument(
            '--use-http-for-dhcp', action='store_true', default=False,
            help=(
                "Fetching the DHCP config via HTTP is quicker, "
                "but we've run into issues with various NIC firmwares "
                "when operating in BIOS mode. As such we default to the slower, "
                "yet more reliable TFTP for BIOS. If a server is known "
                "to be working fine with HTTP, it can be forced with this option."
            )
        )
        parser.add_argument(
            '--force', action='store_true',
            help="Skip the first confirmation prompt, don't ask to --move-vlan, unset --new if host is in PuppetDB.")
        parser.add_argument(
            '--opt82', action='store_true', default=False,
            help='Use DHCP option 82 to identify a physical host rather than its UUID or MAC.')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ReimageRunner(args, self.spicerack)


class ReimageRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements,too-many-branches
        """Initialize the reimage runner."""
        ensure_shell_is_durable()
        self.args = args
        self.host = self.args.host

        if '.' in self.host:
            raise RuntimeError('You need to pass only the host name, not the FQDN.')

        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.host, read_write=True)
        self.netbox_data = self.netbox_server.as_dict()

        if not self.args.force:
            ask_confirmation(f'ATTENTION: Destructive action for {self.host}. Proceed?')
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
        self.populate_puppetdb_attempted = False
        self.use_tftp = not self.args.use_http_for_dhcp

        try:
            self.remote_host = self.remote.query(self.fqdn)
            if self.args.new:
                if not self.args.force:
                    ask_confirmation(f'Host {self.fqdn} was found in PuppetDB but --new was set. Are you sure you want '
                                     'to proceed? The --new option will be unset')
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

        if not self.args.force and not self.virtual and not self.args.move_vlan and \
                self.netbox_server.access_vlan in LEGACY_VLANS and \
                not self.host.startswith('ganeti'):
            ask_confirmation('Physical host on legacy vlan/IP, please consider re-imaging it using --move-vlan.\n'
                             'More info: https://wikitech.wikimedia.org/wiki/Vlan_migration\n'
                             'Continue to ignore.')

        # Do not reimage pooled DBs
        dbctl_instance = spicerack.dbctl().instance.get(self.host)
        if dbctl_instance is not None and any(section['pooled'] for section in dbctl_instance.sections.values()):
            ask_confirmation(f'ATTENTION: host {self.host} is present in dbctl and seems to be currently POOLED. '
                             f'Are you sure you want to proceed?\nCurrent sections config: {dbctl_instance.sections}')

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
        self.puppet_server = spicerack.puppet_server()

        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])

        # Keep track of some specific actions for the eventual rollback
        self.rollback_masks = False
        self.rollback_depool = False

        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        # Load properties / attributes specific to either virtual
        # or physical hosts.
        self.dhcp_config: DHCPConfiguration
        if self.virtual:
            self.ganeti_instance: GntInstance = self.ganeti.instance(self.fqdn)
            self.ganeti_rapi: GanetiRAPI = self.ganeti.rapi(self.ganeti_instance.cluster)
            self.ganeti_data: dict = self.ganeti_rapi.fetch_instance(self.fqdn)
            ganeti_macs = self.ganeti_data.get('nic.macs', None)
            if not ganeti_macs or len(ganeti_macs) != 1:
                raise RuntimeError(f'Unable to get MAC from Ganeti for {self.host}')
            self.mac = ganeti_macs[0]
            self.dhcp_config = self._get_dhcp_config_virtual(mac=self.mac)
        else:
            self.mgmt_fqdn = self.netbox_server.mgmt_fqdn
            self.redfish = spicerack.redfish(self.host)
            self.is_uefi = self.redfish.is_uefi
            if not self.is_uefi:
                self.ipmi: Ipmi = self.spicerack.ipmi(self.mgmt_fqdn)
            # Nokia currently cannot insert the server-connected port in
            # Option 82, so error if --opt82 is set
            nb_switch = self.netbox.api.dcim.devices.get(name=self.netbox_server.switches[0])
            if nb_switch.device_type.manufacturer.slug == "nokia" and self.args.opt82:
                raise RuntimeError('Using --opt82 is incompatible with the Nokia switch connected to this device')
            self.identifier = '' if self.args.opt82 else self._get_host_identifier()
            self.dhcp_config = self._get_dhcp_config_baremetal(force_tftp=self.use_tftp, identifier=self.identifier)

        self._validate()

    def _get_host_identifier(self) -> str:
        # If Dell, generate the uuid from the serial, once all boxes support
        # redfish this can be removed.
        if self.netbox_data['device_type']['manufacturer']['slug'] == DELL_VENDOR_SLUG:
            serial = self.netbox_data['serial']
            hex_string = bytes.hex(b'LLED%c%c%c%c%c%c%c%c%c%c%c%c' % (
                0x00, ord(serial[1]),
                ord(serial[2]), 0x10,
                0x80, ord(serial[3]),
                ord(serial[0]) | 0x80, 0xc0, 0x4f, ord(serial[4]), ord(serial[5]), ord(serial[6]),
            ))
            formatted_hex = (f"{hex_string[:8]}-{hex_string[8:12]}-"
                             f"{hex_string[12:16]}-{hex_string[16:20]}-{hex_string[20:]}")
            return formatted_hex

        # If supermicro (or anything else) try to fetch the UUID from Redfish
        try:
            if self.redfish.uuid:
                return self.redfish.uuid
        except RedfishError as exp:
            logger.error('Unable to obtain the SMBIOS UUID via redfish, using MAC:\n%s\n', exp)

        # If no UUID, last catch all, use the MAC of the PXE nic
        return self._get_primary_mac()

    # To be eventually trimmed if option 97 works well
    def _get_primary_mac(self) -> str:
        """Get the MAC address from redfish or from Netbox"""
        netbox_mac = self.netbox_server.primary_mac_address
        if netbox_mac:
            return netbox_mac
        try:
            redfish_mac = self.redfish.get_primary_mac()
        except RedfishError as exp:
            # TODO: once option 82 is no more, make it a hard blocker
            logger.error(('Non-blocking error while trying to get the primary NIC MAC address:\n%s\n'
                          'Make sure PXE is properly configured and iDRAC up to date\n'), exp)
            redfish_mac = None
        try:
            networking_fact = get_puppet_fact(self.requests, self.host, 'networking')
            puppet_mac = json.loads(networking_fact)['mac'] if networking_fact else None
        except (KeyError, RuntimeError, ValueError) as exp:
            logger.warning('Can\'t get the primary MAC address from PuppetDB:\n%s\n', exp)
            puppet_mac = None
        if puppet_mac and redfish_mac and puppet_mac != redfish_mac:
            logger.error(('The MAC address stored in PuppetDB is different from the one from Redfish (%s vs %s)\n'
                         'Make sure PXE is configure on the proper interface.'),
                         puppet_mac, redfish_mac)
        if redfish_mac:
            return redfish_mac
        if puppet_mac:
            # Temporarily store the primary MAC exposed by Puppet in Netbox, in case the reimage fails
            self.netbox_server.primary_mac_address = puppet_mac
            return puppet_mac
        # Something is very wrong if the MAC is in none of the 3 locations
        logger.error('Can\'t find the host\'s MAC address in Netbox, Redfish or PuppetDB.')
        return ''

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
        if self.populate_puppetdb_attempted:
            logger.info("Attempt to clear Puppetdb's state.")
            self.puppet_server.delete(self.fqdn)

        self.host_actions.failure('**The reimage failed, see the cookbook logs for the details. '
                                  f'You can also try typing "sudo install-console {self.fqdn}" to get a root shell, '
                                  'but depending on the failure this may not work.**')
        logger.error('Reimage executed with errors:\n%s\n', self.actions)
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
            if self.is_uefi:
                self.redfish.check_connection()
            else:
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

    def _get_dhcp_config_virtual(self, mac) -> DHCPConfMac:
        ip = ipaddress.IPv4Interface(self.netbox_data['primary_ip4']['address']).ip

        if not isinstance(ip, ipaddress.IPv4Address):
            raise RuntimeError(f'Unable to find primary IPv4 address for {self.host}.')

        return DHCPConfMac(
            hostname=self.host,
            ipv4=ip,
            mac=mac,
            ttys=0,
            distro=self.args.os
        )

    def _get_dhcp_config_baremetal(
        self, force_tftp: bool = False, identifier: str = ""
    ) -> DHCPConfiguration:
        """Instantiate a DHCP configuration to be used for the reimage."""
        if len(identifier) == 36:
            uuid = identifier
            mac = ''
        elif len(identifier) == 17:
            uuid = ''
            mac = identifier
        else:
            uuid = ''
            mac = ''
        if self.args.pxe_media:
            installer_suffix = f"{self.args.os}-{self.args.pxe_media}"
        else:
            installer_suffix = f"{self.args.os}-installer"
        dhcp_filename_ipxe = ''
        # After the iPXE boot loader is fetched via UEFI HTTP Boot, iPXE
        # tries to fetch autoexec.ipxe from the base directory of the boot
        # loader URL. However, on some Dell servers iPXE is not able to
        # obtain the domain name server, though this works on Supermicro
        # servers and via Grub. Consequently, iPXE is not able to resolve
        # the boot URL and pull down autoexec.ipxe. As a workaround, use
        # the IP:
        # - iPXE bug report: https://github.com/ipxe/ipxe/issues/1316
        # - Failed: Dell R440 Bios version: 2.22.1, purchased 2019-04-01
        # - Succeeded: Dell R450 Bios version: 1.15.2, purchased 2023-04-10
        # We could re-evaluate this hack as old hardware ages out of the
        # fleet.
        apt_ip = self.dns.resolve_ipv4('apt.wikimedia.org')[0]
        # UEFI boot
        if self.is_uefi:
            dhcp_filename = f'http://{apt_ip}/efiboot/snponly.efi'
            dhcp_options = {
                # HACK: root-path is used by ipxe to construct the installer URL
                # we could in the future have ipxe query the os version via some
                # other method.
                'root-path': installer_suffix,
                'vendor-class-identifier': 'HTTPClient',
            }
            # Prevent the debian-installer from receiving the filename in our
            # DHCP offer as the debian-installer will try to load any URL as a
            # preseed config
            dhcp_filename_exclude_vendor = "d-i"
        # Legacy MBR boot
        else:
            # via iPXE
            if uuid:
                dhcp_filename = '/srv/tftpboot/ipxe/undionly.kpxe'
                dhcp_options = {
                    # HACK: root-path is used by ipxe to construct the installer URL
                    # we could in the future have ipxe query the os version via some
                    # other method.
                    'root-path': installer_suffix,
                }
                dhcp_filename_ipxe = f'http://{apt_ip}/efiboot/autoexec.ipxe'
                dhcp_filename_exclude_vendor = ""
            # via Debian's pxelinux
            else:
                # This is a workaround to avoid PXE booting issues, like
                # "Failed to load ldlinux.c32" before getting to Debian Install.
                # More info: https://phabricator.wikimedia.org/T363576#9997915
                # We also got confirmation from Supermicro/Broadcom that they
                # don't support lpxelinux.0, so for this vendor we force the TFTP flag
                # even if it wasn't set.
                if force_tftp or \
                        self.netbox_data['device_type']['manufacturer']['slug'] == SUPERMICRO_VENDOR_SLUG:
                    logger.info('Force pxelinux.0 and TFTP only for DHCP settings.')
                    dhcp_filename = f"/srv/tftpboot/{self.args.os}-installer/pxelinux.0"
                    dhcp_options = {
                        "pxelinux.pathprefix": f"/srv/tftpboot/{installer_suffix}/"
                    }
                else:
                    dhcp_filename = ""
                    dhcp_options = {}
                dhcp_filename_exclude_vendor = ""

        if mac:
            return DHCPConfMac(
                hostname=self.host,
                ipv4=ipaddress.IPv4Interface(self.netbox_server.primary_ip4_address).ip,
                mac=mac,
                ttys=1,
                distro=self.args.os,
                media_type=self.args.pxe_media,
                dhcp_options=dhcp_options,
                dhcp_filename=dhcp_filename,
                dhcp_filename_exclude_vendor=dhcp_filename_exclude_vendor,
            )

        if uuid:
            return DHCPConfUUID(
                hostname=self.host,
                ipv4=ipaddress.IPv4Interface(self.netbox_server.primary_ip4_address).ip,
                uuid=uuid,
                ttys=1,
                distro=self.args.os,
                media_type=self.args.pxe_media,
                dhcp_options=dhcp_options,
                dhcp_filename=dhcp_filename,
                dhcp_filename_exclude_vendor=dhcp_filename_exclude_vendor,
                dhcp_filename_ipxe=dhcp_filename_ipxe,
            )

        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        netbox_iface = netbox_host.primary_ip.assigned_object
        # If it's a ganeti host the primary IP is on a bridge, we get the physical port that is a member
        if netbox_iface.type.value == "bridge":
            netbox_iface = self.netbox.api.dcim.interfaces.get(
                device_id=netbox_host.id,
                bridge_id=netbox_iface.id,
                type__n=("virtual", "lag", "bridge"),
                mgmt_only=False,
            )
        try:
            switch_iface = netbox_iface.connected_endpoints[0]
        except TypeError as e:
            raise RuntimeError(f'Error finding switch port connected to {netbox_iface} on {netbox_host}. Netbox '
                               'model of server connections is invalid.') from e
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
            dhcp_options=dhcp_options,
            dhcp_filename=dhcp_filename,
            dhcp_filename_exclude_vendor=dhcp_filename_exclude_vendor,
        )

    def _install_os(self):
        """Perform the OS reinstall."""
        pxe_reboot_time = datetime.utcnow()

        # iPXE based boot
        if hasattr(self, 'identifier') and self.identifier:
            di_cmdline_pattern = 'preseed/url='
        # pxelinux based boot
        else:
            di_cmdline_pattern = 'BOOT_IMAGE=debian-installer'

        if self.virtual:
            # Prepare a Ganeti VM for reboot and PXE boot for
            # reimaging.
            self.ganeti_instance.set_boot_media('network')
            self.host_actions.success('Forced PXE for next reboot')
            self.ganeti_instance.shutdown()
            self.ganeti_instance.startup()
            self.host_actions.success('Host rebooted via gnt-instance')
        else:
            # Prepare a physical host for reboot and PXE or UEFI HTTP boot
            # for reimaging.
            if self.is_uefi:
                self.redfish.force_http_boot_once()
                self.host_actions.success('Forced UEFI HTTP Boot for next reboot')
                self.redfish.chassis_reset(ChassisResetPolicy.FORCE_RESTART)
                self.host_actions.success('Host rebooted via Redfish')
            else:
                self.ipmi.force_pxe()
                self.host_actions.success('Forced PXE for next reboot')
                self.ipmi.reboot()
                self.host_actions.success('Host rebooted via IPMI')

        self.remote_installer.wait_reboot_since(pxe_reboot_time, print_progress_bars=False)
        time.sleep(30)  # Avoid race conditions, the host is in the d-i, need to wait anyway
        di_reboot_time = datetime.utcnow()
        env_command = f'grep -q "{di_cmdline_pattern}" /proc/cmdline'
        try:
            self.remote_installer.run_sync(env_command, print_output=False, print_progress_bars=False)
        except RemoteExecutionError:
            ask_confirmation('Unable to verify that the host is inside the Debian installer, please verify manually '
                             f'with: sudo install-console {self.fqdn}')
        self.host_actions.success('Host up (Debian installer)')

        # Reset boot media allowing the newly installed OS to boot.
        if self.virtual:
            self.ganeti_instance.set_boot_media('disk')
            self.host_actions.success('Set boot media to disk')
        else:
            # On UEFI platforms the boot override once flag should be disabled
            # by the UEFI firmware prior to boot. Altering the override flag
            # during the d-i, via Redfish, causes the UEFI boot entries created
            # by grub to be lost, at least on Supermicro hardware. Commit
            # 9d3cdfb8a6a5f1ec34f77839d685bdcbc6f84edd was added to explicitly
            # disable via Redfish during the d-i, because the override flag
            # seemed to not be properly disabled, but it is possible the cause
            # was different, so don't alter for now.
            if not self.is_uefi:
                self.ipmi.remove_boot_override()
                self.ipmi.check_bootparams()
                self.host_actions.success('Checked BIOS boot parameters are back to normal')

        try:
            self.remote_installer.wait_reboot_since(di_reboot_time, print_progress_bars=False)
            self.remote_installer.run_sync(f'! {env_command}', print_output=False, print_progress_bars=False)
        except (RemoteCheckError, RemoteExecutionError):
            ask_confirmation('Unable to verify that the host rebooted into the new OS, it might still be in the '
                             f'Debian installer, please verify manually with: sudo install-console {self.fqdn}')

        result = self.remote_installer.run_sync('lsb_release -sc',
                                                print_output=False, print_progress_bars=False, is_safe=True)

        distro: str = 'unknown'
        for _, output in result:
            distro = output.message().decode()

        if distro != self.args.os and not self.spicerack.dry_run:
            message = f'New OS is {distro} but {self.args.os} was requested'
            self.host_actions.failure(message)
            raise RuntimeError(message)

        self.host_actions.success(f'Host up (new fresh {distro} OS)')

    def _populate_puppetdb(self):
        """Run Puppet in noop mode to populate the exported resources in PuppetDB to downtime it on Icinga."""
        self.populate_puppetdb_attempted = True
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
                    ["=", "type", "Nagios_host"]
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
        self.populate_puppetdb_attempted = False  # No need to remove it from PuppetDB past this point

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

        # Workaround bug https://github.com/netbox-community/pynetbox/issues/586 by refreshing netbox_server
        # Otherwise pynetbox fails at setting it to None as it thinks it's already None
        self.netbox_server = self.spicerack.netbox_server(self.netbox_data['name'], read_write=True)
        if self.netbox_server.primary_mac_address:
            # Systematically clear the MAC address stored in Netbox on successful reimage
            self.netbox_server.primary_mac_address = None

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
        print(f'Starting reimage on {self.host}. You can check progress via serial console '
              f'or by running `install-console {self.fqdn}` on any cumin host')

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

        if self.args.move_vlan:
            move_vlan_retcode = self.spicerack.run_cookbook(
                'sre.hosts.move-vlan', ['reimage', self.host])
            if move_vlan_retcode == 0:
                self.host_actions.success('Host successfully migrated to the new VLAN')
            else:
                self.host_actions.failure('**Failed to migrate host to the new VLAN, '
                                          f'sre.hosts.move-vlan cookbook returned {move_vlan_retcode}**')
                raise RuntimeError(f'sre.hosts.move-vlan cookbook returned {move_vlan_retcode}')
            # Update the DHCP config with the New IP
            self.dhcp_config = self._get_dhcp_config_baremetal(force_tftp=self.use_tftp, identifier=self.identifier)
            self._validate()

        # Clear both old Puppet5 and new Puppet7 infra in all cases, it doesn't fail if the host is not present
        self.puppet_server.delete(self.fqdn)

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

        # Comment on the Phabricator task
        logger.info('Reimage completed:\n%s\n', self.actions)
        self.phabricator.task_comment(
            self.args.task_id,
            (f'Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:\n'
             f'{self.actions}\n'),
        )

        if self.host_actions.has_failures:
            return 1

        return 0
