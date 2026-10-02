"""Set the DHCP temporary config for the given host."""
import ipaddress
import json
import logging
from typing import cast

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.dhcp import DHCPConfOpt82, DHCPConfMac, DHCPConfMgmt, DHCPConfiguration, DHCPError, DHCPConfUUID
from spicerack.ganeti import Ganeti, GanetiRAPI, GntInstance
from spicerack.netbox import MANAGEMENT_IFACE_NAME
from spicerack.redfish import RedfishError
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.hosts import OS_VERSIONS, SUPERMICRO_VENDOR_SLUG, DELL_VENDOR_SLUG
from cookbooks.sre.puppet import get_puppet_fact

logger = logging.getLogger(__name__)

# Lengths of the two accepted host identifiers, used to tell them apart.
UUID_LENGTH = 36
MAC_ADDRESS_LENGTH = 17


class Dhcp(CookbookBase):
    """Set the ephemeral DHCP for a given host, then give control to the user and clear the DHCP config on exit.

    With neither --push nor --remove the config is kept in place until the user gives back control, and
    removed on continuation. The --push and --remove modes perform a single action and exit, so that the
    DHCP config can be managed in isolation (debug sessions, provisioning, etc..).

    Usage:
        cookbook sre.hosts.dhcp --os bookworm example1001
        cookbook sre.hosts.dhcp --mgmt --push example1001
        cookbook sre.hosts.dhcp --mgmt --remove example1001
    """

    owner_team = "Infrastructure Foundations"

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('--os', choices=OS_VERSIONS,
                           help='the Debian version to install. One of %(choices)s.')
        group.add_argument(
            '--mgmt', action='store_true', default=False,
            help="Create the DHCP config for the BMC's management interface."
        )
        parser.add_argument(
            '--pxe-media', default='installer',
            help=('Specify a different media suffix to use in the PXE settings of the DHCP configuration. To be used '
                  'when a specific installer is needed that is available as tftpboot/$OS-$PXE_MEDIA/.'))
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
        action_group = parser.add_mutually_exclusive_group()
        action_group.add_argument(
            '--push', action='store_true', default=False,
            help="Force the cookbook to just push the DHCP config to the install servers."
        )
        action_group.add_argument(
            '--remove', action='store_true', default=False,
            help="Force the cookbook to just remove the DHCP config from the install servers."
        )
        parser.add_argument('host', help='Short hostname of the host for which to set the DHCP config, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return DhcpRunner(args, self.spicerack)


class DhcpRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the DHCP runner."""
        ensure_shell_is_durable()
        self.args = args

        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.args.host)
        self.virtual: bool = self.netbox_server.virtual
        self.netbox_data = self.netbox_server.as_dict()
        self.spicerack = spicerack

        # Shortcut variables
        self.host = self.args.host
        self.fqdn = self.netbox_server.fqdn
        self.use_tftp = not self.args.use_http_for_dhcp
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        self.requests = spicerack.requests_session(__name__, timeout=(5.0, 30.0))

        if self.virtual:
            ganeti: Ganeti = spicerack.ganeti()
            ganeti_instance: GntInstance = ganeti.instance(self.fqdn)
            ganeti_rapi: GanetiRAPI = ganeti.rapi(ganeti_instance.cluster)
            ganeti_data: dict = ganeti_rapi.fetch_instance(self.fqdn)
            ganeti_macs = ganeti_data.get('nic.macs', None)
            if not ganeti_macs or len(ganeti_macs) != 1:
                raise RuntimeError(f'Unable to get MAC from Ganeti for {self.host}')
            mac_address = ganeti_macs[0]
            self.dhcp_config: DHCPConfiguration = self._get_dhcp_config_mac(mac_address)
            logging.info("Ganeti virt detected, using its mac address %s", mac_address)
        else:
            self.redfish = spicerack.redfish(self.args.host)
            self.vendor_slug = self.netbox_data["device_type"]["manufacturer"]["slug"]
            if args.mgmt:
                self.dhcp_config = self._get_mgmt_dhcp_config()
            else:
                self.is_uefi = self.redfish.is_uefi
                self.dhcp_config = self._get_dhcp_config_baremetal(
                    force_tftp=self.use_tftp, identifier=self._get_host_identifier()
                )

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        if self.args.push:
            action = "push config only"
        elif self.args.remove:
            action = "remove config only"
        else:
            action = "live debug"
        return f"for host {self.fqdn} ({action})"

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=self.host, concurrency=1, ttl=3600)

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

    def _get_dhcp_filename_ipxe(self, uefi: bool) -> str:
        """Helper to create the IPXE's dhcp filename."""
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
        apt_ip = self.spicerack.dns().resolve_ipv4('apt.wikimedia.org')[0]
        if uefi:
            return f'http://{apt_ip}/efiboot/snponly.efi'
        return f'http://{apt_ip}/efiboot/autoexec.ipxe'

    def _get_mgmt_dhcp_config(self):
        """Returns a DHCP config for the management interface."""
        mgmt_fqdn = self.netbox_server.mgmt_fqdn
        if self.vendor_slug == SUPERMICRO_VENDOR_SLUG:
            # TODO: move to Spicerack
            mgmt_iface = self.netbox.api.dcim.interfaces.get(device=self.host, name=MANAGEMENT_IFACE_NAME)
            if mgmt_iface is None or not mgmt_iface.mac_address:
                raise RuntimeError(
                    f'Unable to find the MAC address of the {MANAGEMENT_IFACE_NAME} interface of {self.host} in '
                    'Netbox. Ask DCOps to populate it before provisioning this host.'
                )
            logger.info("Using the BMC's MAC address for the DHCP config.")
            return DHCPConfMac(
                hostname=mgmt_fqdn,
                ipv4=cast(ipaddress.IPv4Address, self.redfish.interface.ip),
                mac=mgmt_iface.mac_address,
                ttys=0,
                distro="",
            )
        else:
            return DHCPConfMgmt(
                datacenter=self.netbox_data['site']['slug'],
                serial=self.netbox_data['serial'],
                manufacturer=self.vendor_slug,
                fqdn=mgmt_fqdn,
                ipv4=cast(ipaddress.IPv4Address, self.redfish.interface.ip),
            )

    def _get_dhcp_config_mac(self, mac) -> DHCPConfMac:
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

    def _get_dhcp_config_baremetal(  # pylint: disable=too-many-locals
        self, force_tftp: bool = False, identifier: str = ""
    ) -> DHCPConfiguration:
        """Instantiate a DHCP configuration to be used for the reimage."""
        if len(identifier) == UUID_LENGTH:
            uuid = identifier
            mac = ''
        elif len(identifier) == MAC_ADDRESS_LENGTH:
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
        # UEFI boot
        if self.is_uefi:
            dhcp_filename = self._get_dhcp_filename_ipxe(uefi=True)
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
                dhcp_filename_ipxe = self._get_dhcp_filename_ipxe(uefi=False)
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
        # Reload the host from Netbox
        #  In case its IP changed with --move-vlan
        #  And also used lower down for `netbox_iface`
        netbox_server = self.spicerack.netbox_server(self.host, read_write=True)
        if mac:
            return DHCPConfMac(
                hostname=self.host,
                ipv4=ipaddress.IPv4Interface(netbox_server.primary_ip4_address).ip,
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
                ipv4=ipaddress.IPv4Interface(netbox_server.primary_ip4_address).ip,
                uuid=uuid,
                ttys=1,
                distro=self.args.os,
                media_type=self.args.pxe_media,
                dhcp_options=dhcp_options,
                dhcp_filename=dhcp_filename,
                dhcp_filename_exclude_vendor=dhcp_filename_exclude_vendor,
                dhcp_filename_ipxe=dhcp_filename_ipxe,
            )
        # TODO: remove direct Netbox API calls as soon as we decom DHCP option 82
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
            ipv4=ipaddress.IPv4Interface(netbox_server.primary_ip4_address).ip,
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

    def run(self):
        """Set the DHCP config and give control to the user."""

        dc = self.netbox_data["site"]["slug"]
        common_msg = (
            "Temporary DHCP config for host %s has been %s on the install host(s) in "
            "the datacenter %s under /etc/dhcp/automation."
        )
        try:
            if self.args.push:
                self.dhcp.push_configuration(self.dhcp_config)
                logger.info(common_msg, self.fqdn, "pushed", dc)
            elif self.args.remove:
                self.dhcp.remove_configuration(self.dhcp_config)
                logger.info(common_msg, self.fqdn, "removed", dc)
            else:
                with self.dhcp.config(self.dhcp_config):
                    logger.info(common_msg, self.fqdn, "pushed", dc)
                    ask_confirmation("The DHCP setting will be cleared on continuation. You can debug the host now!")
        except DHCPError as e:
            logger.error("An error has occurred while working on the DHCP config: %s", e)
            return 1
