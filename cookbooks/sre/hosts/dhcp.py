"""Set the DHCP temporary config for the given host."""
import ipaddress
import logging
from typing import cast

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.dhcp import DHCPConfOpt82, DHCPConfMac, DHCPConfMgmt, DHCPError
from spicerack.netbox import MANAGEMENT_IFACE_NAME
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.hosts import OS_VERSIONS, SUPERMICRO_VENDOR_SLUG

logger = logging.getLogger(__name__)


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
                "to be working fine with HTTP, it can be forced with this option.")
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
        self.netbox_data = self.netbox_server.as_dict()

        # Shortcut variables
        self.host = self.args.host
        if self.netbox_server.virtual:
            raise RuntimeError(f'Host {self.host} is a virtual machine. VMs are not yet supported.')

        self.fqdn = self.netbox_server.fqdn
        self.vendor_slug = self.netbox_data["device_type"]["manufacturer"]["slug"]
        self.redfish = spicerack.redfish(self.host)
        self.use_tftp = not self.args.use_http_for_dhcp
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        if args.mgmt:
            self.dhcp_config = self._get_mgmt_dhcp_config()
        else:
            self.dhcp_config = self._get_dhcp_config()

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

    def _get_dhcp_config(self):
        """Returns a DHCP config for the primary interface."""
        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        switch_iface = netbox_host.primary_ip.assigned_object.connected_endpoints[0]
        if switch_iface is None:  # Temporary workaround to support Ganeti hosts
            ifaces = self.netbox.api.dcim.interfaces.filter(device=netbox_host.name, mgmt_only=False)
            connected_ifaces = [iface for iface in ifaces if iface.connected_endpoints]
            if len(connected_ifaces) == 1:
                switch_iface = connected_ifaces[0].connected_endpoints[0]
            else:
                raise RuntimeError(f'Unable to find the switch interface to which {self.host} is connected to. The '
                                   f'interfaces that are connected in Netbox are: {connected_ifaces}')

        switch_hostname = (
            switch_iface.device.virtual_chassis.name.split('.')[0]
            if switch_iface.device.virtual_chassis is not None
            else switch_iface.device.name
        )

        # This is a workaround to avoid PXE booting issues, like
        # "Failed to load ldlinux.c32" before getting to Debian Install.
        # More info: https://phabricator.wikimedia.org/T363576#9997915
        # We also got confirmation from Supermicro/Broadcom that they
        # don't support lpxelinux.0, so for this vendor we force the TFTP flag
        # even if it wasn't set.
        if self.use_tftp:
            dhcp_filename = f"/srv/tftpboot/{self.args.os}-installer/pxelinux.0"
            dhcp_options = {
                "pxelinux.pathprefix": f"/srv/tftpboot/{self.args.os}-installer/"
            }
        else:
            dhcp_filename = ""
            dhcp_options = {}

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
