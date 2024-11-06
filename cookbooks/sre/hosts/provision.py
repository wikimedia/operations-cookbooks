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
from cookbooks.sre.hosts import (
    SUPERMICRO_VENDOR_SLUG,
    DELL_VENDOR_SLUG,
)
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

SUPERMICRO_AMD_DEVICE_SLUGS = (
    'as-2014s-tr',
)

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
        parser.add_argument('--uefi', action='store_true', help='Set boot mode to UEFI and HTTP')
        parser.add_argument('host', help='Short hostname of the host to provision, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        netbox_server = self.spicerack.netbox_server(args.host)
        netbox_data = netbox_server.as_dict()
        if netbox_server.virtual:
            raise RuntimeError(f'Host {args.host} is a virtual machine. VMs are not supported.')

        # Sanity checks before proceeding with any Runner
        if netbox_server.status == 'active' and (not args.no_dhcp or not args.no_users):
            raise RuntimeError(
                f'Host {args.host} has active status in Netbox but --no-dhcp and --no-users were not set.')
        if args.host.startswith(VIRT_PREFIXES) and not args.enable_virtualization:
            raise RuntimeError(
                'Virtualization not enabled but this host will need it.')

        # The Runner to instantiate is vendor-specific to ease the customizations
        # and management of different vendors via Redfish.
        vendor = netbox_data['device_type']['manufacturer']['slug']
        if vendor == SUPERMICRO_VENDOR_SLUG:
            return SupermicroProvisionRunner(args, self.spicerack)
        if vendor == DELL_VENDOR_SLUG:
            return DellProvisionRunner(args, self.spicerack)
        raise RuntimeError(f"The vendor {vendor} is currently not supported.")


class SupermicroProvisionRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements, too-many-branches
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
        self.device_model_slug = self.netbox_data['device_type']['slug']

        if self.args.no_users:
            bmc_username = "root"
            password = ''  # nosec
        else:
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

        # DHCP automation
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        logger.info("Using the BMC's MAC address for the DHCP config.")
        self.dhcp_config: Union[DHCPConfMac, DHCPConfMgmt] = DHCPConfMac(
            hostname=self.fqdn,
            ipv4=self.redfish.interface.ip,
            mac=self.netbox.api.dcim.interfaces.get(device=self.args.host, name=MANAGEMENT_IFACE_NAME).mac_address,
            ttys=0,
            distro="",
        )
        self._dhcp_active = False

        if self.netbox_server.status in ('active', 'staged'):
            self.chassis_reset_policy = ChassisResetPolicy.GRACEFUL_RESTART
        else:
            self.chassis_reset_policy = ChassisResetPolicy.FORCE_RESTART

        self.mgmt_password = spicerack.management_password

        self.mgmt_network_changes = {
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
            },
            'DHCPv4': {
                'DHCPEnabled': False,
            }
        }

        # From various tests it seems that the value of BootModeSelect
        # (EFI/Legacy) varies the allowed values of other BIOS options as well.
        # The idea is to patch these settings in a first round, wait for them
        # to be picked up and then do another round of patch settings (to allow
        # proper values to be selected).
        # Please do not add any EFI/Boot/etc.. related setting in here.
        # More info: https://phabricator.wikimedia.org/T365372#10213162
        self.bios_changes = {
            "Attributes": {
                "BootModeSelect": 'UEFI' if self.args.uefi else 'Legacy',
                "ConsoleRedirection": False,
                "QuietBoot": False,
                "LegacySerialRedirectionPort": "COM1",
            }
        }

        # Some Supermicro BIOS settings differ on servers with AMD CPUs.
        intel_virt_flag = "Enable" if self.args.enable_virtualization else "Disable"
        amd_virt_flag = "Enabled" if self.args.enable_virtualization else "Disabled"
        if self.device_model_slug not in SUPERMICRO_AMD_DEVICE_SLUGS:
            self.bios_changes["Attributes"]["SerialPort2Attribute"] = "SOL"
            self.bios_changes["Attributes"]["IntelVirtualizationTechnology"] = intel_virt_flag
        else:
            self.bios_changes["Attributes"]["SVMMode"] = amd_virt_flag

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
                raise RuntimeError(error_msg) from e

        confirm_on_failure(check_connection)

        self._config_host()

        if self._detect_hw_raid():
            action = ask_input(
                'Detected Hardware RAID. Please configure the RAID '
                'at this point (the password is still the default one). '
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

        if not self.args.no_dhcp:
            self.dhcp.remove_configuration(self.dhcp_config)
            self._dhcp_active = False

        self.redfish.check_connection()
        if self.args.no_users:
            logger.info('Skipping root user password change')
        else:
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
        """Get if a hardware raid configuration is set for a Supermicro host."""
        try:
            storage = self.redfish.request('get', self.redfish.storage_manager).json()
            has_raid = False
            for storage_member in storage['Members']:
                if storage_member['@odata.id'].split('/')[-1].startswith('HA-RAID'):
                    has_raid = True
                    break
        except Exception:  # pylint: disable=broad-except
            logger.warning('Unable to detect if there is Hardware RAID on the host, ASSUMING IT HAS RAID.')
            has_raid = True
        return has_raid

    def _get_bios_settings(self):
        try:
            logger.info("Retrieving updated BIOS settings...")
            bios_settings = self.redfish.request(
                'GET',
                '/redfish/v1/Systems/1/Bios',
            ).json()
            return bios_settings["Attributes"]
        except RedfishError as e:
            logger.error("Error while retrieving BIOS settings: %s", e)
            return {}

    def _patch_bios_settings(self):
        logger.info("Applying BIOS settings...")
        self.redfish.request(
            'PATCH',
            '/redfish/v1/Systems/1/Bios',
            json=self.bios_changes
        )

    def _reboot_chassis(self):
        logger.info(
            'Rebooting the host with policy %s and waiting for 5 minutes', self.chassis_reset_policy
        )
        self.redfish.chassis_reset(self.chassis_reset_policy)
        sleep(300)

    def _config_host(self):
        """Provision the BIOS and BMC settings."""
        try:
            logging.info("Retrieving the BMC's firmware version.")
            bmc_response = self.redfish.request("get", "/redfish/v1/UpdateService/FirmwareInventory/BMC").json()
            logging.info("BMC firmware release date: %s", bmc_response['ReleaseDate'])
            if bmc_response['ReleaseDate'].startswith('2022-'):
                ask_confirmation(
                    "The BMC firmware was released in 2022 and it may not support "
                    "all the settings that we need. Please consider upgrading firmware "
                    "first. See https://phabricator.wikimedia.org/T371416 for more info.")
            logging.info("Retrieving BIOS settings (first round).")
            bios_attributes = self._get_bios_settings()
            logging.info("Setting up BootMode and basic BIOS settings.")
            should_patch = self._found_diffs_bios_attributes(bios_attributes)
            if should_patch:
                logger.info(
                    "Found differences between our desired status and the current "
                    "one, applying new BIOS settings (a reboot will be performed).")
                self._patch_bios_settings()
                self._reboot_chassis()
            else:
                logger.info(
                    "No BIOS settings applied since the config is already good.")

            logging.info("Retrieving BIOS settings (second round).")
            bios_attributes = self._get_bios_settings()
            # Note: It seems that Supermicro's BIOS settings assume
            # PXE via EFI configs, so we force 'Legacy' in all BIOS settings
            # having 'EFI' has value. It should be enough to force PXE via IPMI,
            # without setting any specific boot order.
            # More info: https://phabricator.wikimedia.org/T365372#10148864
            self._config_pxe_bios_settings(bios_attributes)
            should_patch = self._found_diffs_bios_attributes(bios_attributes)
            if should_patch:
                logger.info(
                    "Found differences between our desired status and the current "
                    "one, applying new BIOS settings (a reboot will be performed).")
                self._patch_bios_settings()
            else:
                logger.info(
                    "No BIOS settings applied since the config is already good.")

            logger.info("Applying Network changes to the BMC.")
            self.redfish.request(
                'PATCH',
                '/redfish/v1/Managers/1/EthernetInterfaces/1',
                json=self.mgmt_network_changes
            )
            # As precaution we reboot after the BMC network settings are applied,
            # even if not strictly needed.
            if should_patch:
                self._reboot_chassis()
        except RedfishError as e:
            raise RuntimeError(
                f"Error while configuring BIOS or mgmt interface: {e}") from e

    def _found_diffs_bios_attributes(self, bios_attributes: dict):
        """Diff the Supermicro's BIOS settings/attributes with our ideal config."""
        found_diffs = False
        for key, value in self.bios_changes["Attributes"].items():
            try:
                if not bios_attributes[key] == value:
                    logger.info(
                        "BIOS: %s is set to %s, while we want %s",
                        key, bios_attributes[key], value
                    )
                    found_diffs = True
            except KeyError as e:
                logger.info(
                    "BIOS: %s is not present in the current settings.", key)
                raise RuntimeError(
                    f"Error while checking BIOS attribute {key}") from e
        return found_diffs

    def _config_pxe_bios_settings(self, bios_attributes: dict):
        """Set BIOS settings from EFI to Legacy, including riser's PCIe settings.

        Look for all BIOS settings with a value containing 'EFI' and set them
        to Legacy. This is needed to allow NIC ports to PXE correctly in our
        environment. Set also all options starting with "RSC_" to Legacy as well,
        since the nomenclature is used for PCIe riser NICs.
        """
        if self.args.uefi:
            old_value = "Legacy"
            new_value = "EFI"
            self.bios_changes["Attributes"]['IPv4HTTPSupport'] = 'Enabled'
            self.bios_changes["Attributes"]['IPv4PXESupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6PXESupport'] = 'Disabled'
        else:
            old_value = "EFI"
            new_value = "Legacy"
            self.bios_changes["Attributes"]['IPv4HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv4PXESupport'] = 'Enabled'
            self.bios_changes["Attributes"]['IPv6HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6PXESupport'] = 'Disabled'

        for key, value in bios_attributes.items():
            if old_value == str(value) or key.startswith("RSC_"):
                self.bios_changes["Attributes"][key] = new_value


class DellProvisionRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements, too-many-branches
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
        self.device_model_slug = self.netbox_data['device_type']['slug']

        if self.args.no_users:
            password = ''  # nosec
        else:
            password = DELL_DEFAULT

        self.redfish = spicerack.redfish(
            self.args.host, username='root', password=password)

        # DHCP automation
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        self.dhcp_config = DHCPConfMgmt(
            datacenter=self.netbox_data['site']['slug'],
            serial=self.netbox_data['serial'],
            manufacturer=self.netbox_data['device_type']['manufacturer']['slug'],
            fqdn=self.fqdn,
            ipv4=self.redfish.interface.ip,
        )
        self._dhcp_active = False

        if self.netbox_server.status in ('active', 'staged'):
            self.reboot_policy = DellSCPRebootPolicy.GRACEFUL
            self.chassis_reset_policy = ChassisResetPolicy.GRACEFUL_RESTART
        else:
            self.reboot_policy = DellSCPRebootPolicy.FORCED
            self.chassis_reset_policy = ChassisResetPolicy.FORCE_RESTART

        self.mgmt_password = spicerack.management_password

        self.platform_doc_link = (
            "https://wikitech.wikimedia.org/wiki/SRE/Dc-operations/"
            "Platform-specific_documentation/Dell_Documentation#Troubleshooting_2"
        )

        # BIOS/iDRAC/etc.. settings for Dell hosts.
        self.config_changes = {
            'BIOS.Setup.1-1': {
                'BootMode': 'Uefi' if self.args.uefi else 'Bios',
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
        return (
            f'for host {self.netbox_server.mgmt_fqdn} with chassis set policy '
            f'{self.chassis_reset_policy.name} and with Dell SCP reboot policy ' f'{self.reboot_policy.name}')

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
                error_msg = (
                    f"Unable to connect to the Redfish API of {self.args.host}. "
                    f"Follow {self.platform_doc_link}"
                )
                raise RuntimeError(error_msg) from e

        confirm_on_failure(check_connection)

        if self._detect_hw_raid():
            action = ask_input(
                'Detected Hardware RAID. Please configure the RAID '
                'at this point (the password is still the default one). '
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

        try:
            self._config_host()
        except Exception:  # pylint: disable=broad-except
            logger.warning('First attempt to load the new configuration failed, auto-retrying once')
            confirm_on_failure(self._config_host)

        if not self.args.no_dhcp:
            self.dhcp.remove_configuration(self.dhcp_config)
            self._dhcp_active = False

        self.redfish.check_connection()
        if self.args.no_users:
            logger.info('Skipping root user password change')
        else:
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
        """Get if a hardware raid configuration is set for a Dell host."""
        try:
            storage = self.redfish.request('get', self.redfish.storage_manager).json()
            has_raid = False
            for storage_member in storage['Members']:
                if storage_member['@odata.id'].split('/')[-1].startswith('RAID'):
                    has_raid = True
                    break
        except Exception:  # pylint: disable=broad-except
            logger.warning('Unable to detect if there is Hardware RAID on the host, ASSUMING IT HAS RAID.')
            has_raid = True
        return has_raid

    def _get_config(self):
        """Get the current BIOS/iDRAC configuration."""
        self.redfish.check_connection()
        return self.redfish.scp_dump(allow_new_attributes=True)

    def _config_host(self):
        """Provision the BIOS and iDRAC settings."""
        config = self._get_config()
        if config.model.lower() in OLD_SERIAL_MODELS:
            self.config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedirCom2'
            self.config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Serial1Com1Serial2Com2'
            self.config_changes['BIOS.Setup.1-1']['InternalUsb'] = 'Off'
        else:
            self.config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedir'
            self.config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Com2'

        if self.args.uefi:
            self.config_changes['BIOS.Setup.1-1']['HttpDev1EnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1DhcpEnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1DnsDhcpEnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1Protocol'] = 'IPv4'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1VlanEnDis'] = 'Disabled'

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
            raise RuntimeError(
                'Not all changes were applied successfully, see the ones '
                'reported above that starts with "Updated value..."'
            )

        logger.info('All changes were applied successfully')

    def _get_pxe_nic(self, nics):
        """Select the NIC to use for PXE booting.

        Arguments:
            nics (list of str): The list of NIC names reported by Redfish.

        Returns:
            pxe_nic (str): The NIC name to use for PXE.

        """
        nics_with_link = []
        nics_failed = []
        for nic in nics:
            try:
                nic_json = self.redfish.request(
                    'GET', f'{self.redfish.system_manager}/EthernetInterfaces/{nic}').json()
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
                    f'Unable to auto-detect NIC with link. Pick the one to set PXE on:\n{nics}', nics)

        if not pxe_nic:
            if len(nics_with_link) == 1:
                pxe_nic = nics_with_link[0]
            elif nics_with_link:
                pxe_nic = ask_input(
                    f'Detected link on {len(nics_with_link)} interfaces. Pick the one to set PXE on:\n{nics_with_link}',
                    nics_with_link)
            else:
                pxe_nic = ask_input(
                    f'Unable to auto-detect NIC with link. Pick the one to set PXE on:\n{nics}', nics)

        return pxe_nic

    def _config_pxe(self, config):
        """Configure PXE or UEFI HTTP boot on the correct NIC automatically or ask the user if unable to detect it.

        Example keys names for DELL:

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
        if not all_nics:
            raise RuntimeError('Unable to find any NIC.')
        pxe_nic = self._get_pxe_nic(all_nics)
        if self.args.uefi:
            logger.info('Enabling UEFI HTTP boot on NIC %s', pxe_nic)
            self.config_changes['BIOS.Setup.1-1']['HttpDev1Interface'] = pxe_nic
        else:
            logger.info('Enabling PXE boot on NIC %s', pxe_nic)
            for nic in all_nics:
                if nic == pxe_nic:
                    self.config_changes[pxe_nic] = {'LegacyBootProto': 'PXE'}
                else:
                    self.config_changes[nic] = {'LegacyBootProto': 'NONE'}

        # Set SetBootOrderEn to disk, primary NIC
        if self.args.uefi:
            # TODO to be verified
            # Uefi have a dedicated/virtual NIC (HttpDevice), which match HttpDev1Interface
            # As well as a different disk name
            new_order = ['Disk.SATAEmbedded.A-1', 'NIC.HttpDevice.1-1']
        else:
            new_order = ['HardDisk.List.1-1', pxe_nic]
        # SetBootOrderEn defaults to comma-separated, but some hosts might differ
        separator = ', ' if ', ' in config.components['BIOS.Setup.1-1']['SetBootOrderEn'] else ','
        self.config_changes['BIOS.Setup.1-1']['SetBootOrderEn'] = separator.join(new_order)
        if self.args.uefi:
            uefi_boot_seq = config.components['BIOS.Setup.1-1'].get('UefiBootSeq', ', ')
            # on my test host UefiBootSeq have a space after the coma while SetBootOrderEn doesn't
            separator = ',' if ',' in uefi_boot_seq and ', ' not in uefi_boot_seq else ', '
            self.config_changes['BIOS.Setup.1-1']['UefiBootSeq'] = separator.join(new_order)
        else:
            # BiosBootSeq defaults to comma-space-separated, but some hosts might differ
            # Use a default if the host is in UEFI mode and dosn't have the setting at all.
            bios_boot_seq = config.components['BIOS.Setup.1-1'].get('BiosBootSeq', ', ')
            separator = ',' if ',' in bios_boot_seq and ', ' not in bios_boot_seq else ', '
            self.config_changes['BIOS.Setup.1-1']['BiosBootSeq'] = separator.join(new_order)
