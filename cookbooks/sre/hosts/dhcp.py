"""Set the DHCP temporary config for the given host."""
import ipaddress

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.dhcp import DHCPConfOpt82
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.hosts import OS_VERSIONS


class Dhcp(CookbookBase):
    """Set the ephemeral DHCP for a given host, then give control to the user and clear the DHCP config on exit.

    Usage:
        cookbook sre.hosts.dhcp --os buster example1001
    """

    owner_team = "Infrastructure Foundations"

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. One of %(choices)s')
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
        self.fqdn = self.netbox_server.fqdn
        self.remote = spicerack.remote()
        if self.netbox_server.virtual:
            raise RuntimeError(f'Host {self.host} is a virtual machine. VMs are not yet supported.')

        self.remote_host = self.remote.query(f'D{{{self.fqdn}}}')
        self.use_tftp = not self.args.use_http_for_dhcp
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        self.dhcp_config = self._get_dhcp_config()

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.fqdn}'

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=self.host, concurrency=1, ttl=3600)

    def _get_dhcp_config(self):
        """Instantiate a DHCP configuration to be used."""
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
        with self.dhcp.config(self.dhcp_config):
            ask_confirmation(
                f'Temporary DHCP config for host {self.fqdn} has been setup on the install host(s) in the '
                f'{self.netbox_data["site"]["slug"]} datacenter in /etc/dhcp/automation. The DHCP setting will be '
                'cleared on continuation. You can debug the host now!'
            )
