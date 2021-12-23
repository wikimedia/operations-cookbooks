"""Provision a new physical host setting up it's BIOS and management console."""
import argparse
import ipaddress
import logging

from pprint import pformat

from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.dhcp import DHCPConfMgmt
from spicerack.redfish import DellSCPPowerStatePolicy, DellSCPRebootPolicy
from spicerack.remote import RemoteError
from wmflib.interactive import ask_confirmation, ask_input, confirm_on_failure, ensure_shell_is_durable


DNS_ADDRESS = '10.3.0.1'
DELL_DEFAULT = 'calvin'
logger = logging.getLogger(__name__)


class Provision(CookbookBase):
    """Provision a new physical host setting up it's BIOS, management console and NICs.

    Actions performed:
        * Validate that the host is a physical host and the vendor is supported (only Dell at this time)
        * Fail if the host is active on Netbox but --no-dhcp and --no-users are not set as a precautionary measure
        * [unless --no-dhcp is set] Setup the temporary DHCP so that the management console can get a connection and
          become reachable
        * Get the current configuration for BIOS, management console and NICs
        * Modify the common settings
          * [if --enable-virtualization is set] Leave virtualization enabled, by default it gets disabled
        * Push back the whole modified configuration
        * Checks that it can still connect to Redfish API
        * Checks that the configuration has been applied correctly dumping the new configuration and trying to apply
          the same changes. In case it detects any non-applied configuration will prompt the user what to do. It can
          retry to apply them, or the user can apply them manually (via web console or ssh) and then skip the step.
        * [unless --no-users is set] Update the root's user password with the production management password
        * Checks that it can connect via remote IPMI

    Usage:
        cookbook sre.hosts.provision example1001
        cookbook sre.hosts.provision --enable-virtualization example1001
        cookbook sre.hosts.provision --no-dhcp --no-users example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            '--no-dhcp',
            action='store_true',
            help='Skips the DHCP setting, assuming that the management console is already reachable')
        parser.add_argument(
            '--no-users',
            action='store_true',
            help=("Skips changing the root's user password from Dell's default value to the management one. Uses the "
                  "management passwords also for the first connection"))
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

        if self.netbox_server.status == 'active' and (not self.args.no_dhcp or not self.args.no_users):
            raise RuntimeError(
                f'Host {self.args.host} has active status in Netbox but --no-dhcp and --no-users were not set.')

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
        if self.args.no_users:
            password = ''  # nosec
        else:
            password = DELL_DEFAULT

        if self.netbox_server.status in ('active', 'staged'):
            self.reboot_policy = DellSCPRebootPolicy.GRACEFUL
        else:
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
        """Run the cookbook."""
        if self.args.no_dhcp:
            confirm_on_failure(self._config)
        else:
            with self.dhcp.config(self.dhcp_config):
                confirm_on_failure(self._config)

        self.redfish.check_connection()
        if self.args.no_users:
            logger.info('Skipping root user password change')
        else:
            self.redfish.change_user_password('root', self.mgmt_password)

        self.ipmi.check_connection()

    def _get_config(self):
        """Get the current BIOS/iDRAC configuration."""
        self.redfish.check_connection()
        return self.redfish.scp_dump()

    def _config(self):
        """Provision the BIOS and iDRAC settings."""
        config = self._get_config()
        self._config_pxe(config)
        was_changed = config.update(self.config_changes)
        if not was_changed:
            logger.warning('Skipping update of BIOS/iDRAC, all settings have already the correct values')
            return

        if self.redfish.get_power_state() == DellSCPPowerStatePolicy.OFF.value:
            power_state = DellSCPPowerStatePolicy.OFF
        else:
            power_state = DellSCPPowerStatePolicy.ON

        response = self.redfish.scp_push(config, reboot=self.reboot_policy, preview=False, power_state=power_state)
        logger.debug('SCP import results:\n%s', pformat(response))

        logger.info('Checking if all the changes were applied successfully')
        config = self._get_config()
        was_changed = config.update(self.config_changes)
        if was_changed:
            raise RuntimeError('Not all changes were applied successfully, see the ones reported above that starts '
                               'with "Updated value..."')

        logger.info('All changes were applied successfully')

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
        embedded_nics = sorted(key for key in config.components.keys() if key.startswith('NIC.Embedded.'))
        other_nics = sorted(
            key for key in config.components.keys() if key.startswith('NIC.') and key not in embedded_nics)

        prefixes = {key.split('-')[0] for key in other_nics}
        if len(prefixes) == 1 and self.multi_gigabit:
            if self.multi_gigabit:  # One external card and multi-gigabit set on Netbox, select the external NIC
                pxe_nic = other_nics[0]
            else:  # One external card but multi-gigabit not set on Netbox, select theembedded NIC
                pxe_nic = embedded_nics[0]
        elif not prefixes and not self.multi_gigabit:  # Just embedded NICs and multi-gigabit not set on Netbox
            pxe_nic = embedded_nics[0]
        else:  # Unable to auto-detect
            all_nics = embedded_nics + other_nics
            all_nics_list = '\n'.join(all_nics)
            speed = 'multi-gigabit' if self.multi_gigabit else '1G'
            pxe_nic = ask_input(
                f'Unable to auto-detect NIC, Netbox reports {speed} NIC. Pick the one to set PXE on:\n{all_nics_list}',
                all_nics)

        logger.info('Enabling PXE boot on NIC %s', pxe_nic)
        self.config_changes['NIC.Embedded.1-1-1'] = {'LegacyBootProto': 'NONE'}
        self.config_changes[pxe_nic] = {'LegacyBootProto': 'PXE'}

        # Set SetBootOrderEn to disk, primary NIC
        new_order = ','.join(['HardDisk.List.1-1', pxe_nic])
        self.config_changes['BIOS.Setup.1-1']['SetBootOrderEn'] = new_order
        if self.config_changes['BIOS.Setup.1-1'].get('BiosBootSeq', ''):  # Some models don't have this key
            self.config_changes['BIOS.Setup.1-1']['BiosBootSeq'] = new_order
