"""Provision a new physical host setting up it's BIOS and management console."""
import logging

from pprint import pformat
from time import sleep
from typing import Union

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.dhcp import DHCPConfMac, DHCPConfMgmt
from spicerack.netbox import MANAGEMENT_IFACE_NAME
from spicerack.redfish import ChassisResetPolicy, DellSCPPowerStatePolicy, DellSCPRebootPolicy, RedfishError
from wmflib.interactive import ask_confirmation, ask_input, confirm_on_failure, get_secret, ensure_shell_is_durable
from cookbooks.sre.network import configure_switch_interfaces

DNS_ADDRESS = '10.3.0.1'
DELL_DEFAULT = 'calvin'
OLD_SERIAL_MODELS = (
    'poweredge r430',
    'poweredge r440',
    'poweredge r630',
    'poweredge r640',
    'poweredge r730',
    'poweredge r730xd',
    'poweredge r740xd',
    'poweredge r740xd2',
)
DELL_VENDOR_SLUG = 'dell'
SUPERMICRO_VENDOR_SLUG = 'supermicro'
SUPPORTED_VENDORS = [DELL_VENDOR_SLUG, SUPERMICRO_VENDOR_SLUG]
# Hostname prefixes that usually need --enable-virtualization
VIRT_PREFIXES = ('ganeti', 'cloudvirt')
logger = logging.getLogger(__name__)


class Provision(CookbookBase):
    """Provision a new physical host setting up it's BIOS, management console, NICs, network.

    Actions performed:
        * Validate that the host is a physical host and the vendor is supported (only Dell at this time)
        * Fail if the host is active on Netbox but --no-dhcp and --no-users are not set as a precautionary measure
        * [unless --no-dhcp is set] Setup the temporary DHCP so that the management console can get a connection and
          become reachable
        * Detect if the host has hardware RAID, if so ask the operator to configure it before proceeding and reboot
          the host if the RAID was modified.
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
        * Configures the network switch

    Usage:
        cookbook sre.hosts.provision example1001
        cookbook sre.hosts.provision --enable-virtualization example1001
        cookbook sre.hosts.provision --no-dhcp --no-users example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            '--no-dhcp',
            action='store_true',
            help='Skips the DHCP setting, assuming that the management console is already reachable')
        parser.add_argument(
            '--no-switch',
            action='store_true',
            help='Skips the network switch config')
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
        self.verbose = spicerack.verbose
        if self.netbox_server.virtual:
            raise RuntimeError(f'Host {self.args.host} is a virtual machine. VMs are not supported.')

        self.vendor = self.netbox_data['device_type']['manufacturer']['slug']
        if self.vendor not in SUPPORTED_VENDORS:
            vendor = self.netbox_data['device_type']['manufacturer']['name']
            raise RuntimeError(
                f'Host {self.args.host} manufacturer is {vendor}. '
                f'The only supported ones are {SUPPORTED_VENDORS}.')

        if self.vendor == SUPERMICRO_VENDOR_SLUG:
            ask_confirmation(
                "The host's vendor is Supermicro, we have limited support "
                "for it. Please check the following Phabricator task for more info: "
                "https://phabricator.wikimedia.org/T365372. Please also report "
                "in there any anomaly/doubt/etc.. so that SRE Infrastructure "
                "Foundations will be aware. Thanks!"
            )

        if self.netbox_server.status == 'active' and (not self.args.no_dhcp or not self.args.no_users):
            raise RuntimeError(
                f'Host {self.args.host} has active status in Netbox but --no-dhcp and --no-users were not set.')

        if self.args.no_users:
            password = ''  # nosec
        else:
            if self.vendor == DELL_VENDOR_SLUG:
                bmc_username = "root"
                password = DELL_DEFAULT
            if self.vendor == SUPERMICRO_VENDOR_SLUG:
                # The Supermicro vendor ships its servers with a unique BMC admin
                # password, that is displayed in the server's label:
                # https://www.supermicro.com/en/support/BMC_Unique_Password
                # To keep it simple, for the moment we just ask the password
                # to the admin/operator running the cookbook.
                # The correspondent user is "ADMIN".
                bmc_username = "ADMIN"
                logger.info(
                    "Supermicro sets a unique BMC ADMIN password for every server, "
                    "usually printed in the server's label or collected in "
                    "a spreadsheet."
                    "====== IMPORTANT =====:\n"
                    "If this is the first time ever that the host "
                    "is provisioned, please retrieve the password and insert it "
                    "so the cookbook can change it to ours.\n"
                    "If this is not the first time, and the ADMIN password "
                    "has already been changed to the standard root one, "
                    "please insert that instead.")
                password = get_secret("BMC ADMIN Password")

        self.redfish = spicerack.redfish(
            self.args.host, username=bmc_username, password=password)

        if self.args.host.startswith(VIRT_PREFIXES) and not self.args.enable_virtualization:
            ask_confirmation("Virtualization not enabled but this host might need it, continue?")

        # DHCP automation
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        if self.vendor == SUPERMICRO_VENDOR_SLUG:
            logger.info("Using the BMC's MAC address for the DHCP config.")
            self.dhcp_config: Union[DHCPConfMac, DHCPConfMgmt] = DHCPConfMac(
                hostname=self.fqdn,
                ipv4=self.redfish.interface.ip,
                mac=self.netbox.api.dcim.interfaces.get(device=self.args.host, name=MANAGEMENT_IFACE_NAME).mac_address,
                ttys=0,
                distro="",
            )
        else:
            self.dhcp_config = DHCPConfMgmt(
                datacenter=self.netbox_data['site']['slug'],
                serial=self.netbox_data['serial'],
                manufacturer=self.netbox_data['device_type']['manufacturer']['slug'],
                fqdn=self.fqdn,
                ipv4=self.redfish.interface.ip,
            )
        self._dhcp_active = False

        if self.netbox_server.status in ('active', 'staged'):
            self.dell_reboot_policy = DellSCPRebootPolicy.GRACEFUL
            self.chassis_reset_policy = ChassisResetPolicy.GRACEFUL_RESTART
        else:
            self.dell_reboot_policy = DellSCPRebootPolicy.FORCED
            self.chassis_reset_policy = ChassisResetPolicy.FORCE_RESTART

        self.mgmt_password = spicerack.management_password

        self.dell_platform_doc_link = (
            "https://wikitech.wikimedia.org/wiki/SRE/Dc-operations/"
            "Platform-specific_documentation/Dell_Documentation#Troubleshooting_2"
        )

        # BIOS/iDRAC/etc.. settings for Dell hosts.
        self.dell_config_changes = {
            'BIOS.Setup.1-1': {
                'BootMode': 'Bios',
                'CpuInterconnectBusLinkPower': 'Enabled',
                'EnergyPerformanceBias': 'BalancedPerformance',
                'PcieAspmL1': 'Enabled',
                'ProcC1E': 'Enabled',
                'ProcCStates': 'Enabled',
                'ProcPwrPerf': 'OsDbpm',
                'ProcVirtualization': 'Enabled' if self.args.enable_virtualization else 'Disabled',
                'ProcX2Apic': 'Disabled',
                'SysProfile': 'PerfPerWattOptimizedOs',
                'UncoreFrequency': 'DynamicUFS',
                'UsbPorts': 'OnlyBackPortsOn',
            },
            'iDRAC.Embedded.1': {
                'IPMILan.1#Enable': 'Enabled',
                'IPv4.1#DHCPEnable': 'Disabled',
                'IPv4Static.1#Address': str(self.redfish.interface.ip),
                'IPv4Static.1#DNS1': DNS_ADDRESS,
                'IPv4Static.1#Gateway': str(next(self.redfish.interface.network.hosts())),
                'IPv4Static.1#Netmask': str(self.redfish.interface.netmask),
                'NIC.1#DNSRacName': self.args.host,
                'NICStatic.1#DNSDomainFromDHCP': 'Disabled',
                'NICStatic.1#DNSDomainName': f'mgmt.{self.netbox_data["site"]["slug"]}.wmnet',
                'WebServer.1#HostHeaderCheck': 'Enabled',
            },
            'System.Embedded.1': {
                'ServerPwr.1#PSRapidOn': 'Disabled',
            }
        }

        self.supermicro_mgmt_network_changes = {
            "HostName": self.args.host,
            "FQDN": self.fqdn,
            "IPv4StaticAddresses": [{
                "Address": str(self.redfish.interface.ip),
                "Gateway": str(next(self.redfish.interface.network.hosts())),
                "SubnetMask": str(self.redfish.interface.netmask)
            }],
            "StaticNameServers": [DNS_ADDRESS],
            "StatelessAddressAutoConfig": {
                'IPv6AutoConfigEnabled': False
            }
        }

        self.supermicro_bios_changes = {
            "Attributes": {
                "BootModeSelect": "Legacy",
                "ConsoleRedirection": True,
                "IntelVirtualizationTechnology": "Disable",
                "QuietBoot": False,
                "SerialPort2Attribute": "COM",
            }
        }

        # Testing that the management password is correct connecting to the first physical cumin host
        cumin_host = str(next(self.netbox.api.dcim.devices.filter(name__isw='cumin', status='active')))
        try:
            spicerack.redfish(cumin_host).check_connection()
        except RedfishError:
            raise RuntimeError(
                f'The management password provided seems incorrect, it does not work on {cumin_host}.') from None

        ask_confirmation(f'Are you sure to proceed to apply BIOS/iDRAC settings {self.runtime_description}?')

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        descr = (
            f'for host {self.netbox_server.mgmt_fqdn} with chassis set policy '
            f'{self.chassis_reset_policy.name}')
        if self.vendor == DELL_VENDOR_SLUG:
            descr += f'and with Dell SCP reboot policy {self.dell_reboot_policy.name}'
        return descr

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=self.args.host, concurrency=1, ttl=1800)

    def run(self):
        """Run the cookbook."""
        if not self.args.no_switch:
            configure_switch_interfaces(self.remote, self.netbox, self.netbox_data, self.verbose)

        if not self.args.no_dhcp:
            self.dhcp.push_configuration(self.dhcp_config)
            self._dhcp_active = True

        def check_connection():
            try:
                self.redfish.check_connection()
            except RedfishError as e:
                error_msg = f"Unable to connect to the Redfish API of {self.args.host}. "
                if self.vendor == DELL_VENDOR_SLUG:
                    error_msg += f"Follow {self.dell_platform_doc_link}"
                raise RuntimeError(error_msg) from e

        confirm_on_failure(check_connection)

        if self.vendor == SUPERMICRO_VENDOR_SLUG:
            self._config_supermicro_host()

        if self._detect_hw_raid():
            action = ask_input(
                'Detected Hardware RAID. Please configure the RAID '
                f'at this point (the password is still {self.vendor} default one). '
                'Once done select "modified" if the RAID was modified '
                'or "untouched" if it was not touched. '
                'If the RAID was modified the host will be rebooted to '
                'make sure the changes are applied.',
                ('untouched', 'modified')
            )

            if action == 'modified':
                logger.info(
                    'Rebooting the host with policy %s and waiting for 3 minutes', self.chassis_reset_policy
                )
                self.redfish.chassis_reset(self.chassis_reset_policy)
                # TODO: replace the sleep with auto-detection of the completiono of the RAID job.
                sleep(180)

        if self.vendor == DELL_VENDOR_SLUG:
            try:
                self._config_dell_host()
            except Exception:  # pylint: disable=broad-except
                logger.warning('First attempt to load the new configuration failed, auto-retrying once')
                confirm_on_failure(self._config_dell_host)

        if not self.args.no_dhcp:
            self.dhcp.remove_configuration(self.dhcp_config)
            self._dhcp_active = False

        self.redfish.check_connection()
        if self.args.no_users:
            logger.info('Skipping root user password change')
        else:
            if self.vendor == SUPERMICRO_VENDOR_SLUG:
                try:
                    self.redfish.find_account("root")
                except RedfishError as e:
                    logger.info(
                        "The root user on the BMC has not been created yet. "
                        "More info: %s", e)
                    logger.info(
                        'Creating the root user on the BMC.')
                    self.redfish.add_account('root', self.mgmt_password)
                logger.info(
                    "Updating the ADMIN user's password on the BMC.")
                self.redfish.change_user_password('ADMIN', self.mgmt_password)
            logger.info(
                "Updating the root user's password on the BMC.")
            self.redfish.change_user_password('root', self.mgmt_password)

        sleep(10)  # Trying to avoid a race condition that seems to make IPMI fail right after changing the password
        self.ipmi.check_connection()

    def rollback(self):
        """Rollback the DHCP setup if present."""
        if self._dhcp_active:
            logger.info('Rolling back DHCP setup')
            self.dhcp.remove_configuration(self.dhcp_config)

    def _detect_hw_raid(self):
        """Get if a hardware raid configuration is set for a Dell/Supermicro host."""
        try:
            storage = self.redfish.request('get', self.redfish.storage_manager).json()
            has_raid = False
            # TODO: The assumption is that both Dell's and Supermicro's
            # "Members" field have the same format. We still don't have examples
            # to check, so this needs to be revisited.
            for storage_member in storage['Members']:
                if storage_member['@odata.id'].split('/')[-1].startswith('RAID'):
                    has_raid = True
                    break
        except Exception:  # pylint: disable=broad-except
            logger.warning('Unable to detect if there is Hardware RAID on the host, ASSUMING IT HAS RAID.')
            has_raid = True
        return has_raid

    def _get_dell_config(self):
        """Get the current BIOS/iDRAC configuration."""
        self.redfish.check_connection()
        return self.redfish.scp_dump(allow_new_attributes=True)

    def _config_supermicro_host(self):
        """Provision the BIOS and BMC settings."""
        # We could add a patch method/utility in Spicerack to DRY code,
        # this is only a stub/idea to visualize the approach.
        try:
            self.redfish.request(
                'PATCH',
                '/redfish/v1/Systems/1/Bios',
                json=self.supermicro_bios_changes
            )
            self.redfish.request(
                'PATCH',
                '/redfish/v1/Managers/1/EthernetInterfaces/1',
                json=self.supermicro_mgmt_network_changes
            )
            # TODO: Replace this logic with something more flexible and
            # reusable.
            logger.info(
                'Rebooting the host with policy %s and waiting for 3 minutes', self.chassis_reset_policy
            )
            self.redfish.chassis_reset(self.chassis_reset_policy)
            sleep(180)
        except RedfishError as e:
            logger.error("Error while configuring BIOS or mgmt interface: %s", e)

    def _config_dell_host(self):
        """Provision the BIOS and iDRAC settings."""
        config = self._get_dell_config()
        if config.model.lower() in OLD_SERIAL_MODELS:
            self.dell_config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedirCom2'
            self.dell_config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Serial1Com1Serial2Com2'
            self.dell_config_changes['BIOS.Setup.1-1']['InternalUsb'] = 'Off'
        else:
            self.dell_config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedir'
            self.dell_config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Com2'

        self._config_dell_pxe(config)
        was_changed = config.update(self.dell_config_changes)
        if not was_changed:
            logger.warning('Skipping update of BIOS/iDRAC, all settings have already the correct values')
            return

        if self.redfish.get_power_state() == DellSCPPowerStatePolicy.OFF.value:
            power_state = DellSCPPowerStatePolicy.OFF
        else:
            power_state = DellSCPPowerStatePolicy.ON

        response = self.redfish.scp_push(config, reboot=self.dell_reboot_policy, preview=False, power_state=power_state)
        logger.debug('SCP import results:\n%s', pformat(response))

        logger.info('Checking if all the changes were applied successfully')
        config = self._get_dell_config()
        was_changed = config.update(self.dell_config_changes)
        if was_changed:
            raise RuntimeError(
                'Not all changes were applied successfully, see the ones '
                'reported above that starts with "Updated value..."'
            )

        logger.info('All changes were applied successfully')

    def _config_dell_pxe(self, config):  # pylint: disable=too-many-branches
        """Configure PXE boot on the correct NIC automatically or ask the user if unable to detect it.

        Example keys names::

            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Slot.2-1-1', 'NIC.Slot.2-2-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Mezzanine.1-1-1', 'NIC.Mezzanine.1-2-1']
            ['NIC.Embedded.1-1-1', 'NIC.Embedded.2-1-1', 'NIC.Slot.3-1-1', 'NIC.Slot.3-2-1']
            #     10Gb NIC1                 10Gb NIC2             1Gb NIC1                 1Gb NIC 2
            ['NIC.Integrated.1-1-1', 'NIC.Integrated.1-2-1', 'NIC.Integrated.1-3-1', 'NIC.Integrated.1-4-1']

        Arguments:
            config (spicerack.redfish.RedfishDellSCP): the configuration to modify.

        """
        all_nics = sorted(key for key in config.components.keys() if key.startswith('NIC.'))
        nics_with_link = []
        nics_failed = []
        if not all_nics:
            raise RuntimeError('Unable to find any NIC.')

        for nic in all_nics:
            try:
                nic_json = self.redfish.request(
                    'GET', f'/redfish/v1/Systems/System.Embedded.1/EthernetInterfaces/{nic}').json()
                if nic_json.get('LinkStatus', '') == 'LinkUp':
                    nics_with_link.append(nic)
            except RedfishError as e:
                nics_failed.append(nic)
                logger.error('Unable to detect link status for NIC %s: %s', nic, e)

        pxe_nic = ''
        if nics_failed:
            pick = False
            if len(nics_with_link) == 1:
                response = ask_input(
                    f'Detected link on NIC {nics_with_link[0]} but failed to detect link '
                    f'for some NICs: {nics_failed}.\nDo you want to "continue" with NIC '
                    f'{nics_with_link[0]} or "pick" a different one?', ['continue', 'pick'])
                if response == 'continue':
                    pxe_nic = nics_with_link[0]
                else:
                    pick = True

            if len(nics_with_link) != 1 or pick:
                pxe_nic = ask_input(
                    f'Unable to auto-detect NIC with link. Pick the one to set PXE on:\n{all_nics}', all_nics)

        if not pxe_nic:
            if len(nics_with_link) == 1:
                pxe_nic = nics_with_link[0]
            elif nics_with_link:
                pxe_nic = ask_input(
                    f'Detected link on {len(nics_with_link)} interfaces. Pick the one to set PXE on:\n{nics_with_link}',
                    nics_with_link)
            else:
                pxe_nic = ask_input(
                    f'Unable to auto-detect NIC with link. Pick the one to set PXE on:\n{all_nics}', all_nics)

        logger.info('Enabling PXE boot on NIC %s', pxe_nic)
        for nic in all_nics:
            if nic == pxe_nic:
                self.dell_config_changes[pxe_nic] = {'LegacyBootProto': 'PXE'}
            else:
                self.dell_config_changes[nic] = {'LegacyBootProto': 'NONE'}

        # Set SetBootOrderEn to disk, primary NIC
        new_order = ['HardDisk.List.1-1', pxe_nic]
        # SetBootOrderEn defaults to comma-separated, but some hosts might differ
        separator = ', ' if ', ' in config.components['BIOS.Setup.1-1']['SetBootOrderEn'] else ','
        self.dell_config_changes['BIOS.Setup.1-1']['SetBootOrderEn'] = separator.join(new_order)
        # BiosBootSeq defaults to comma-space-separated, but some hosts might differ
        # Use a default if the host is in UEFI mode and dosn't have the setting at all.
        bios_boot_seq = config.components['BIOS.Setup.1-1'].get('BiosBootSeq', ', ')
        separator = ',' if ',' in bios_boot_seq and ', ' not in bios_boot_seq else ', '
        self.dell_config_changes['BIOS.Setup.1-1']['BiosBootSeq'] = separator.join(new_order)
