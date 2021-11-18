"""Set the DHCP temporary config for the given host."""
import argparse
import ipaddress

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.dhcp import DHCPConfOpt82
from spicerack.remote import RemoteError
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.hosts import OS_VERSIONS


class Dhcp(CookbookBase):
    """Set the ephemeral DHCP for a given host, then give control to the user and clear the DHCP config on exit.

    Usage:
        cookbook sre.hosts.dhcp example1001
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. One of %(choices)s')
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
        # DHCP automation
        try:
            self.dhcp_hosts = self.remote.query(f'A:installserver-light and A:{self.netbox_data["site"]["slug"]}')
        except RemoteError:  # Fallback to eqiad's install server if the above fails, i.e. for a new DC
            self.dhcp_hosts = self.remote.query('A:installserver-light and A:eqiad')
        self.dhcp = spicerack.dhcp(self.dhcp_hosts)
        self.dhcp_config = self._get_dhcp_config()

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.fqdn}'

    def _get_dhcp_config(self):
        """Instantiate a DHCP configuration to be used."""
        netbox_host = self.netbox.api.dcim.devices.get(name=self.host)
        switch_iface = netbox_host.primary_ip.assigned_object.connected_endpoint
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

    def run(self):
        """Set the DHCP config and give control to the user."""
        with self.dhcp.config(self.dhcp_config):
            ask_confirmation(
                f'Temporary DHCP config for host {self.fqdn} has been setup on {self.dhcp_hosts} in '
                '/etc/dhcp/automation. The DHCP setting will be cleared on continuation. You can debug the host now!'
            )