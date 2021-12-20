"""Provision a new physical host setting up it's BIOS and management console."""
import argparse
import ipaddress
import logging

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.dhcp import DHCPConfMgmt
from spicerack.redfish import DellSCPPowerStatePolicy, DellSCPRebootPolicy
from spicerack.remote import RemoteError
from wmflib.interactive import ask_confirmation, ask_input, ensure_shell_is_durable


DNS_ADDRESS = '10.3.0.1'
DELL_DEFAULT = 'calvin'
logger = logging.getLogger(__name__)


class Provision(CookbookBase):
    """Provision a new physical host setting up it's BIOS, management console and NICs.

    Actions performed:
        * Validate that the host is a physical host and the vendor is supported (only Dell at this time)
        * Fail if the host is active on Netbox but --existing was not set
        * [unless --existing is set] Setup the temporary DHCP so that the management console can get a connection and
          become reachable
        * Get the current configuration for BIOS, management console and NICs
        * Modify the common settings
        * Push back the whole modified configuration
        * Check that it can still connect to Redfish API
        * [unless --existing is set] Update the root's user password with the production management password
        * Check that it can still connect to Redfish API
        * Check that it can connect via remote IPMI

    Usage:
        cookbook sre.hosts.provision example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            '--existing',
            action='store_true',
            help=('Consider the host already configured, this will assume:\n'
                  '  * To ask for the management password to connect to its Redfish API\n'
                  '  * Skip the DHCP config assuming the management console has already the correct fixed IP and '
                  'network settings\n'
                  '  * Do not change the root user management password'))
        parser.add_argument('--enable-virtualization', action='store_true',
                            help='Keep virtualization capabilities on. They are turned off if not speficied.')
        parser.add_argument('host', help='Short hostname of the host to provision, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ProvisionRunner(args, self.spicerack)


class ProvisionRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.args = args

        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.args.host)
        self.netbox_data = self.netbox_server.as_dict()
        self.fqdn = self.netbox_server.mgmt_fqdn
        self.ipmi = spicerack.ipmi(self.fqdn)
        self.remote = spicerack.remote()
        if self.netbox_server.virtual:
            raise RuntimeError(f'Host {self.args.host} is a virtual machine. VMs are not supported.')

        if self.netbox_data['device_type']['manufacturer']['slug'] != 'dell':
            vendor = self.netbox_data['device_type']['manufacturer']['name']
            raise RuntimeError(f'Host {self.args.host} manufacturer is {vendor}. Only Dell is supported.')

        if self.netbox_server.status == 'active' and not self.args.existing:
            raise RuntimeError(f'Host {self.args.host} has active status in Netbox but --existing was not set.')

        # DHCP automation
        try:
            self.dhcp_hosts = self.remote.query(f'A:installserver-light and A:{self.netbox_data["site"]["slug"]}')
        except RemoteError:  # Fallback to eqiad's install server if the above fails, i.e. for a new DC
            self.dhcp_hosts = self.remote.query('A:installserver-light and A:eqiad')

        self.dhcp = spicerack.dhcp(self.dhcp_hosts)
        address = self.netbox.api.ipam.ip_addresses.get(dns_name=self.fqdn).address
        self.interface = ipaddress.ip_interface(address)
        self.dhcp_config = DHCPConfMgmt(
            datacenter=self.netbox_data['site']['slug'],
            serial=self.netbox_data['serial'],
            fqdn=self.fqdn,
            ipv4=self.interface.ip,
        )
        if self.args.existing:
            password = ''  # nosec
            self.reboot_policy = DellSCPRebootPolicy.GRACEFUL
        else:
            password = DELL_DEFAULT
            self.reboot_policy = DellSCPRebootPolicy.FORCED

        self.redfish = spicerack.redfish(self.fqdn, 'root', password)
        self.mgmt_password = spicerack.management_password

        self.config_changes = {
            'BIOS.Setup.1-1': {
                'BootMode': 'Bios',
                'CpuInterconnectBusLinkPower': 'Enabled',
                'EnergyPerformanceBias': 'BalancedPerformance',
                'InternalUsb': 'Off',
                'PcieAspmL1': 'Enabled',
                'ProcC1E': 'Enabled',
                'ProcCStates':  'Enabled',
                'ProcPwrPerf': 'OsDbpm',
                'ProcVirtualization': 'Enabled' if self.args.enable_virtualization else 'Disabled',
                'ProcX2Apic': 'Disabled',
                'SerialComm': 'OnConRedirCom2',
                'SerialPortAddress': 'Serial1Com1Serial2Com2',
                'SysProfile': 'PerfPerWattOptimizedOs',
                'UncoreFrequency': 'DynamicUFS',
                'UsbPorts': 'OnlyBackPortsOn',
            },
            'iDRAC.Embedded.1': {
                'IPMILan.1#Enable': 'Enabled',
                'IPv4.1#DHCPEnable': 'Disabled',
                'IPv4Static.1#Address': str(self.interface.ip),
                'IPv4Static.1#DNS1': DNS_ADDRESS,
                'IPv4Static.1#Gateway': str(next(self.interface.network.hosts())),
                'IPv4Static.1#Netmask': str(self.interface.netmask),
                'NICStatic.1#DNSDomainFromDHCP': 'Disabled',
            },
            'System.Embedded.1': {
                'ServerPwr.1#PSRapidOn': 'Disabled',
            }
        }

        netbox_host = self.netbox.api.dcim.devices.get(name=self.args.host)
        self.multi_gigabit = False
        if 'gbase-' in netbox_host.primary_ip.assigned_object.type.value:
            logger.info('Detected multi-gigabit interface, will add specific settings.')
            self.multi_gigabit = True

        ask_confirmation(f'Are you sure to proceed to apply BIOS/iDRAC settings {self.runtime_description}?')

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.netbox_server.mgmt_fqdn} with reboot policy {self.reboot_policy.name}'

    def run(self):
        """Provision BIOS and iDRAC settings."""
        with self.dhcp.config(self.dhcp_config):
            self.redfish.check_connection()
            config = self.redfish.scp_dump()
            if self.multi_gigabit:
                self._config_pxe(config)

            config.update(self.config_changes)

            if self.redfish.get_power_state() == DellSCPPowerStatePolicy.OFF:
                power_state = DellSCPPowerStatePolicy.OFF
            else:
                power_state = DellSCPPowerStatePolicy.ON

            self.redfish.scp_push(config, reboot=self.reboot_policy, preview=False, power_state=power_state)

        self.redfish.check_connection()
        if not self.args.existing:
            self.redfish.change_user_password('root', self.mgmt_password)

        self.redfish.check_connection()
        self.ipmi.check_connection()

    def _config_pxe(self, config):
        """Configure PXE boot on the correct NIC automatically or ask the user if unable to detect it.

        Example keys names::

            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Slot.2-1-1', 'NIC.Slot.2-2-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Mezzanine.1-1-1', 'NIC.Mezzanine.1-2-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Slot.3-1-1', 'NIC.Slot.3-2-1']

        Arguments:
            config (spicerack.redfish.RedfishDellSCP): the configuration to modify.

        """
        embedded_nics = [key for key in config.components.keys() if key.startswith('NIC.Embedded.')]
        other_nics = sorted(
            key for key in config.components.keys() if key.startswith('NIC.') and key not in embedded_nics)

        if not other_nics:
            logger.warning('Multi-gigabit primary interface found on Netbox but no external NIC found. Assuming the '
                           'embedded NICs are the correct one to set PXE on.')
            return

        prefixes = {key.split('-')[0] for key in other_nics}
        if len(prefixes) > 1:
            all_nics = embedded_nics + other_nics
            all_nics_list = '\n'.join(all_nics)
            pxe_nic = ask_input(
                f'Too many NICs found. Pick the one to set PXE on:\n{all_nics_list}', all_nics)
        else:
            pxe_nic = other_nics[0]

        logger.info('Enabling PXE boot on NIC %s', pxe_nic)
        self.config_changes['NIC.Embedded.1-1-1'] = {'LegacyBootProto': 'NONE'}
        self.config_changes[pxe_nic] = {'LegacyBootProto': 'PXE'}

        current_order_parts = config.components['BIOS.Setup.1-1']['SetBootOrderEn'].split(',')
        new_order_parts = []
        for part in current_order_parts:
            if part == 'NIC.Embedded.1-1-1':
                new_order_parts.append(pxe_nic)
            elif part == 'NIC.Embedded.2-1-1':
                continue
            else:
                new_order_parts.append(part)

        new_order = ','.join(new_order_parts)
        self.config_changes['BIOS.Setup.1-1']['SetBootOrderEn'] = new_order
        if self.config_changes['BIOS.Setup.1-1'].get('BiosBootSeq', ''):
            # Some models don't have this key
            self.config_changes['BIOS.Setup.1-1']['BiosBootSeq'] = new_order
