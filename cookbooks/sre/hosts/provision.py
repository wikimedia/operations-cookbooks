"""Provision a new physical host setting up it's BIOS and management console."""
# pylint: disable=too-many-lines
import logging

from collections import defaultdict
from pprint import pformat
from time import sleep
from typing import Union, cast
from ipaddress import IPv4Address
from abc import ABCMeta

from spicerack.apiclient import APIClientResponseError
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.dhcp import DHCPConfMac, DHCPConfMgmt
from spicerack.netbox import MANAGEMENT_IFACE_NAME
from spicerack.redfish import (
    ChassisResetPolicy,
    DellSCPPowerStatePolicy,
    DellSCPRebootPolicy,
    Redfish,
    RedfishDell,
    RedfishError,
    RedfishSupermicro,
)
from wmflib.interactive import ask_confirmation, ask_input, confirm_on_failure, get_secret, ensure_shell_is_durable
from cookbooks.sre.hosts import (
    SUPERMICRO_VENDOR_SLUG,
    DELL_VENDOR_SLUG,
    reboot_chassis
)
from cookbooks.sre.network import configure_switch_interfaces, run_homer

DNS_ADDRESS = '10.3.0.1'
DELL_DEFAULT = 'calvin'
SUPERMICRO_DEFAULT = 'calvin'
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
    'as-8125gs-tnmr2',
)

# T387577
SUPERMICRO_PXE_BUG_SLUGS = (
    'sys-120c-tr-configc',
)

# The Supermicro Config A hosts don't accept the "PXE" setting
# when configuring the NIC ports in the BIOS. They need "Legacy".
SUPERMICRO_CONFIG_A_PXE_LEGACY_SLUGS = (
    'sys-110p-wtr-configa',
)

SUPERMICRO_UEFI_ONLY = (
    'as-8125gs-tnmr2',
)

SUPERMICRO_UEFI_LONG_BOOT_TIME = (
    'as-8125gs-tnmr2',
)

SUPERMICRO_NO_FQDN_MANAGEMENT = (
    'ssg-521e-e1cr24h-configj',
    'as-8125gs-tnmr2',
    'sys-111c-nr',
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

    owner_team = "Infrastructure Foundations"

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
        parser.add_argument('--legacy', action='store_true', help='Set boot mode to Legacy / MBR.')
        parser.add_argument('--homer', action='store_true', help='Use Homer to configure the switches')
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


class ProvisionRunner(CookbookRunnerBase, metaclass=ABCMeta):  # pylint: disable=too-many-instance-attributes
    """Shared Dell and Supermicro logic"""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        self.args = args
        self.dry_run = spicerack.dry_run
        self.netbox = spicerack.netbox()
        self.netbox_server = spicerack.netbox_server(self.args.host)
        self.netbox_data = self.netbox_server.as_dict()
        self.fqdn = self.netbox_server.mgmt_fqdn
        self.ipmi = spicerack.ipmi(self.fqdn)
        self.remote = spicerack.remote()
        self.vendor = self.netbox_data['device_type']['manufacturer']['slug']
        self.verbose = spicerack.verbose
        self.device_model_slug = self.netbox_data['device_type']['slug']
        self.mgmt_password = spicerack.management_password()
        self.uefi = not self.args.legacy
        if self.netbox_server.status in ('active', 'staged'):
            self.chassis_reset_policy = ChassisResetPolicy.GRACEFUL_RESTART
        else:
            self.chassis_reset_policy = ChassisResetPolicy.FORCE_RESTART
        self.redfish: Redfish

    def run(self):
        """Run common switch setup"""
        if not self.args.no_switch:
            # Find switch vendor, as we force Homer usage if it is Nokia
            nb_switch = self.netbox.api.dcim.devices.get(name=self.netbox_server.switches[0])
            if self.args.homer or nb_switch.device_type.manufacturer.slug == "nokia":
                # TODO: doesn't work for virtual-chassis
                run_homer(queries=[f'{hostname}.*' for hostname in self.netbox_server.switches],
                          dry_run=self.dry_run)
            else:
                configure_switch_interfaces(self.remote, self.netbox, self.netbox_data, self.verbose)


class SupermicroProvisionRunner(ProvisionRunner):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    redfish: RedfishSupermicro

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements, too-many-branches
        """Initiliaze the provision runner."""
        super().__init__(args, spicerack)
        ensure_shell_is_durable()
        self.spicerack = spicerack
        self.bmc_firmware_filename = None
        self.bios_firmware_filename = None

        self.uefi_only_devices = [
            # https://phabricator.wikimedia.org/T378368
            "P1_AIOMAOC_ATGC_i2TMLAN1OPROM"
        ]

        if self.device_model_slug in SUPERMICRO_PXE_BUG_SLUGS and not self.uefi:
            ask_confirmation(
                "Due to T387577, during the first configuration of the server "
                "please run provision twice to set PXE to the right NIC/port.")

        # Init redfish with a fake password, since in __init__ we just need
        # some metadata about the host like IP etc..
        # The real initialization happens in run().
        self.redfish: RedfishSupermicro = spicerack.redfish(self.args.host, 'fake')

        # DHCP automation
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        logger.info("Using the BMC's MAC address for the DHCP config.")
        self.dhcp_config: Union[DHCPConfMac, DHCPConfMgmt] = DHCPConfMac(
            hostname=self.fqdn,
            ipv4=cast(IPv4Address, self.redfish.interface.ip),
            mac=self.netbox.api.dcim.interfaces.get(device=self.args.host, name=MANAGEMENT_IFACE_NAME).mac_address,
            ttys=0,
            distro="",
        )
        self._dhcp_active = False

        self.mgmt_network_changes = {
            "HostName": self.args.host,
            "IPv4StaticAddresses": [{
                "Address": str(self.redfish.interface.ip),
                "Gateway": str(next(iter(self.redfish.interface.network.hosts()))),
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

        # TODO: make this check a dynamic one
        # The main issue at the moment is that the "FQDN" key is present
        # and set to "None", but trying to set it leads to an exception
        # for some reason.
        if self.device_model_slug not in SUPERMICRO_NO_FQDN_MANAGEMENT:
            self.mgmt_network_changes["FQDN"] = self.fqdn

        # From various tests it seems that the value of BootModeSelect
        # (EFI/Legacy) varies the allowed values of other BIOS options as well.
        # The idea is to patch these settings in a first round, wait for them
        # to be picked up and then do another round of patch settings (to allow
        # proper values to be selected).
        # Please do not add any EFI/Boot/etc.. related setting in here.
        # More info: https://phabricator.wikimedia.org/T365372#10213162
        self.bios_changes = {
            "Attributes": {
                "QuietBoot": False,
                "LegacySerialRedirectionPort": "COM1",
            }
        }
        if self.device_model_slug not in SUPERMICRO_UEFI_ONLY:
            self.bios_changes["Attributes"]["BootModeSelect"] = "UEFI" if self.uefi else "Legacy"

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
        super().run()

        if not self.args.no_dhcp:
            self.dhcp.push_configuration(self.dhcp_config)
            self._dhcp_active = True

        if not self.args.no_users:
            confirm_on_failure(self._try_bmc_password)
        else:
            # Initialize redfish with the WMF mgmt password
            self.redfish = self.spicerack.redfish(self.args.host)
            confirm_on_failure(self.redfish.check_connection)

        logging.info("Retrieving the BMC's firmware version.")
        bmc_response = self.redfish.request(
            "get", f"{self.redfish.update_service}/FirmwareInventory/BMC").json()
        logging.info("BMC firmware release date: %s", bmc_response['ReleaseDate'])
        if bmc_response['ReleaseDate'].startswith('2022-'):
            ask_confirmation(
                "The BMC firmware was released in 2022 and it may not support "
                "all the settings that we need. Please consider upgrading firmware "
                "first. See https://phabricator.wikimedia.org/T371416 for more info.")
        logging.info("Retrieving the BIOS's firmware version.")
        bios_response = self.redfish.request(
            "get", f"{self.redfish.update_service}/FirmwareInventory/BIOS").json()
        logging.info("BIOS firmware version: %s", bios_response['Version'])
        # We save the BMC/BIOS firmware filename since it is easier and more
        # precise to pin-point corner cases when dealing with BIOS settings
        # later on.
        self.bmc_firmware_filename = bmc_response["Oem"]["Supermicro"]["UniqueFilename"]
        self.bios_firmware_filename = bios_response["Oem"]["Supermicro"]["UniqueFilename"]

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
                reboot_chassis(self.vendor, self.redfish)

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
                "Updating the root user's password on the BMC.")
            self.redfish.change_user_password('root', self.mgmt_password)

            try:
                self.redfish.find_account("ADMIN")
                logger.info(
                    "Updating the ADMIN user's password on the BMC.")
                self.redfish.change_user_password('ADMIN', self.mgmt_password)
            except RedfishError as e:
                logger.info(
                    "The ADMIN user on the BMC is not present, skipping. "
                    "More info: %s", e)

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
        # The timeout was increased in T394357 since sretest2010
        # took more time than the default 10 seconds to handle a PATCH
        # call for BIOS settings.
        self.redfish.request(
            'PATCH',
            '/redfish/v1/Systems/1/Bios',
            json=self.bios_changes,
            timeout=30
        )

    def _get_network_info(self):
        """Find registered network NICs and their port link statuses."""
        network_info: dict = defaultdict(dict)
        network_adapters = self.redfish.request(
            "GET", "/redfish/v1/Chassis/1/NetworkAdapters").json()
        for member in network_adapters["Members"]:
            network_adapter_uri = member["@odata.id"]
            network_adapter = self.redfish.request("GET", network_adapter_uri).json()
            logger.info("Retrieving port configs for %s", network_adapter["Model"])
            network_ports = self.redfish.request(
                "GET", f"{network_adapter_uri}/Ports").json()
            for port in network_ports["Members"]:
                port_info = self.redfish.request("GET", port["@odata.id"]).json()
                model = network_adapter["Model"]
                if "LinkStatus" in port_info:
                    network_info[model][port_info["PortId"]] = port_info["LinkStatus"]
                else:
                    network_info[model][port_info["PortId"]] = "Unknown"
                logger.info(
                    "Port %s has link status %s",
                    port_info["PortId"], network_info[model][port_info["PortId"]])
        return network_info

    def _print_network_info(self, network_info):
        """Pretty print the dictionary returned by _get_network_info."""
        logger.info("NetworkAdapters - Link status for the NIC ports:")
        for model, ports in network_info.items():
            for port, link_status in ports.items():
                logger.info("Model %s Port %s: %s", model, port, link_status)

    def _find_bios_nic_setting(self, model, port, pxe_nic_devices):
        """Find the BIOS NIC setting name from a model and port combination."""
        # For more details see https://phabricator.wikimedia.org/T387577#10607565
        normalized_model = model.replace("-", "_")[0:12]
        port_suffix = f"LAN{port}"
        for nic_device in pxe_nic_devices:
            if normalized_model in nic_device and port_suffix in nic_device:
                return nic_device
        return None

    def _config_host(self):
        """Provision the BIOS and BMC settings."""
        try:
            logging.info("Retrieving BIOS settings (first round).")
            bios_attributes = self._get_bios_settings()
            logging.info("Setting up BootMode and basic BIOS settings.")
            should_patch = self._found_diffs_bios_attributes(bios_attributes)
            if should_patch:
                logger.info(
                    "Found differences between our desired status and the current "
                    "one, applying new BIOS settings (a reboot will be performed).")
                self._patch_bios_settings()
                reboot_chassis(self.vendor, self.redfish)
            else:
                logger.info(
                    "No BIOS settings applied since the config is already good.")

            logging.info("Retrieving BIOS settings (second round).")
            bios_attributes = self._get_bios_settings()

            # Configure BIOS settings to enable/disable HTTP support during PXE.
            self._configure_pxe_http_settings()

            if "HTTPBootPolicy" in bios_attributes:
                self.bios_changes["Attributes"]["HTTPBootPolicy"] = "Apply to each LAN"

                # The CSMSupport option enables the support of MBR in UEFI systems.
                # https://en.wikipedia.org/wiki/UEFI#CSM_booting
                # If this option is not enabled then Supermicro does not present
                # the option to switch to Legacy boot, i.e. it is required for MBR mode
                if "CSMSupport" in bios_attributes:
                    if self.uefi:
                        self.bios_changes["Attributes"]["CSMSupport"] = "Disabled"
                    else:
                        self.bios_changes["Attributes"]["CSMSupport"] = "Enabled"

                # Note: It seems that Supermicro's BIOS settings assume
                # PXE via EFI configs, so we force 'Legacy' in all BIOS settings
                # having 'EFI' has value. It should be enough to force PXE via IPMI,
                # without setting any specific boot order.
                # More info: https://phabricator.wikimedia.org/T365372#10148864
                self._config_pxe_bios_settings(bios_attributes)

            if "ConsoleRedirection" not in bios_attributes:
                self.bios_changes["Attributes"]["COM1ConsoleRedirection"] = False
                self.bios_changes["Attributes"]["SOL_COM2ConsoleRedirection"] = True
            else:
                self.bios_changes["Attributes"]["ConsoleRedirection"] = False

            should_patch = self._found_diffs_bios_attributes(bios_attributes)

            logger.info("Applying Network changes to the BMC.")
            self.redfish.request(
                'PATCH',
                '/redfish/v1/Managers/1/EthernetInterfaces/1',
                json=self.mgmt_network_changes
            )
            # As precaution we reboot after the BMC network settings are applied,
            # even if not strictly needed.
            if should_patch:
                logger.info(
                    "Found differences between our desired status and the current "
                    "one, applying new BIOS settings (a reboot will be performed).")
                self._patch_bios_settings()
                reboot_chassis(self.vendor, self.redfish)
            else:
                logger.info(
                    "No BIOS settings applied since the config is already good.")
        except RedfishError as e:
            ipv6_disabled_err = (
                "The property StatelessAddressAutoConfig is a "
                "read only property and cannot be assigned a value.")
            if (e.__cause__ is not None
                    and isinstance(e.__cause__, APIClientResponseError)
                    and e.__cause__.response is not None  # pylint: disable=no-member
                    and ipv6_disabled_err in str(e.__cause__.response.text)):  # pylint: disable=no-member
                ask_confirmation(
                    "The BMC's IPv6 Stateless autoconfig cannot be disabled. "
                    "This is a bug tracked in T389950, the current workaround "
                    "is to connect to the WebUI and explicitly enable IPv6 "
                    "in the BMC Network config. Then please retry running "
                    "the cookbook, the error shouldn't surface again.")
            else:
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

    def _configure_pxe_http_settings(self):
        """Set BIOS settings related to HTTP PXE support."""
        if self.uefi:
            self.bios_changes["Attributes"]['IPv4HTTPSupport'] = 'Enabled'
            self.bios_changes["Attributes"]['IPv4PXESupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6PXESupport'] = 'Disabled'
        else:
            self.bios_changes["Attributes"]['IPv4HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv4PXESupport'] = 'Enabled'
            self.bios_changes["Attributes"]['IPv6HTTPSupport'] = 'Disabled'
            self.bios_changes["Attributes"]['IPv6PXESupport'] = 'Disabled'

    def _config_pxe_bios_settings(self, bios_attributes: dict):  # pylint: disable=too-many-branches, too-many-locals
        """Set NIC BIOS settings to PXE.

        Legacy Mode:

        Configure the NIC which has link to PXE boot.

        UEFI Mode:

        Supermicro uses a shared UEFI driver to PXE boot all NICs. As a result
        only one NIC is shown in the BIOS config when in UEFI mode, regardless
        of the number NICs on the box. We configure that single device to PXE
        boot, which in turn causes all the NICs to attempt PXE booting.
        """
        if self.uefi:
            old_value = "Legacy"
            new_value = "EFI"
        else:
            old_value = "EFI"
            new_value = "Legacy"
        pxe_nic_devices = []
        for key, value in bios_attributes.items():
            if ((old_value == str(value) or key.startswith("RSC_")) and
                    "LAN" not in key):
                if new_value == 'Legacy' and key in self.uefi_only_devices:
                    ask_confirmation(
                        f"The device related to {key} works only with UEFI settings. "
                        "The cookbook will leave its config as is, please proceed "
                        "only if you don't care about the device.")
                else:
                    self.bios_changes["Attributes"][key] = new_value
            if "LAN" in key:
                logger.info("BIOS - Found a NIC device: %s", key)
                pxe_nic_devices.append(key)

        if len(pxe_nic_devices) == 1:
            pxe_nic = pxe_nic_devices[0]
        else:
            network_info = self._get_network_info()
            self._print_network_info(network_info)
            chosen_pxe_nic = None
            nics_with_link_up = 0
            for model, ports in network_info.items():
                for port, link_status in ports.items():
                    if link_status == "LinkUp":
                        logger.info(
                            "NetworkAdapters - Detected link up for NIC %s port %s", model, port)
                        if nics_with_link_up >= 1:
                            logger.warning(
                                "NetworkAdapters - Detected more than one link with LinkUp status. "
                                "PXE settings already assigned to another NIC, "
                                "skipping this one.")
                        chosen_pxe_nic = self._find_bios_nic_setting(
                            model, port, pxe_nic_devices)
                        nics_with_link_up += 1
            if not chosen_pxe_nic:
                chosen_pxe_nic = ask_input(
                    f"The heuristic to map NIC link status info and BIOS NIC device "
                    "settings failed to provide a suggestion. "
                    "Pick the one to set PXE " f"on:\n{pxe_nic_devices}",
                    pxe_nic_devices)
            for nic in pxe_nic_devices:
                if nic == chosen_pxe_nic:
                    pxe_nic = nic
                else:
                    logger.info("Set (PXE) Disabled to the NIC %s", nic)
                    self.bios_changes["Attributes"][nic] = "Disabled"

        logger.info("Set PXE to the NIC %s", pxe_nic)
        if self.device_model_slug in SUPERMICRO_CONFIG_A_PXE_LEGACY_SLUGS:
            legacy_pxe_setting = "Legacy"
        else:
            legacy_pxe_setting = "PXE"
        uefi_pxe_setting = "EFI"
        self.bios_changes["Attributes"][pxe_nic] = uefi_pxe_setting if self.uefi else legacy_pxe_setting

    def _try_bmc_password(self):
        """Test the known BMC passwords, find a working one and configure Redfish."""
        credentials_to_test = {
            "wmf_root_mgmt": ("root", self.mgmt_password),
            "calvin": ("ADMIN", SUPERMICRO_DEFAULT),
            "BMC_LABEL": ("ADMIN", None),
        }
        for label, (bmc_username, bmc_password) in credentials_to_test.items():
            try:
                logger.info(
                    "Connecting to the BMC as user %s (%s)", bmc_username, label)
                if label == "BMC_LABEL":
                    # The Supermicro vendor ships its servers with a unique BMC admin
                    # password, that is displayed in the server's label:
                    # https://www.supermicro.com/en/support/BMC_Unique_Password
                    bmc_password = get_secret(
                        "Please insert the BMC ADMIN Password written on "
                        "the server's label.")
                self.redfish = self.spicerack.redfish(
                    self.args.host, username=bmc_username, password=bmc_password)
                self.redfish.check_connection()
                # We know for sure that this URI requires authentication.
                self.redfish.request(
                    "get", f"{self.redfish.update_service}/FirmwareInventory/BMC").json()
                logger.info("The username/password combination worked.")
                break
            except RedfishError as e:
                if "HTTP 401" in str(e):
                    logger.warning(
                        "Unauthorized response from the BMC when using "
                        "the password %s", label)
                    continue
                raise RuntimeError(
                    "Client Response error when trying to contact the BMC.") from e
        else:
            raise RuntimeError(
                "Tried all the known combinations of user/passwords, please "
                "verify the username settings on the BMC."
            )


class DellProvisionRunner(ProvisionRunner):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):  # pylint: disable=too-many-statements, too-many-branches
        """Initiliaze the provision runner."""
        super().__init__(args, spicerack)
        ensure_shell_is_durable()

        if self.args.no_users:
            password = ''  # nosec
        else:
            password = DELL_DEFAULT

        self.redfish: RedfishDell = spicerack.redfish(
            self.args.host, username='root', password=password)

        # DHCP automation
        self.dhcp = spicerack.dhcp(self.netbox_data["site"]["slug"])
        self.dhcp_config = DHCPConfMgmt(
            datacenter=self.netbox_data['site']['slug'],
            serial=self.netbox_data['serial'],
            manufacturer=self.netbox_data['device_type']['manufacturer']['slug'],
            fqdn=self.fqdn,
            ipv4=cast(IPv4Address, self.redfish.interface.ip),
        )
        self._dhcp_active = False

        self.all_nics: list = []

        if self.netbox_server.status in ('active', 'staged'):
            self.reboot_policy = DellSCPRebootPolicy.GRACEFUL
        else:
            self.reboot_policy = DellSCPRebootPolicy.FORCED

        self.platform_doc_link = (
            "https://wikitech.wikimedia.org/wiki/SRE/Dc-operations/"
            "Platform-specific_documentation/Dell_Documentation#Troubleshooting_2"
        )

        # BIOS/iDRAC/etc.. settings for Dell hosts.
        self.config_changes = {
            'BIOS.Setup.1-1': {
                'BootMode': 'Uefi' if self.uefi else 'Bios',
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
                'IPv4Static.1#Gateway': str(next(iter(self.redfish.interface.network.hosts()))),
                'IPv4Static.1#Netmask': str(self.redfish.interface.netmask),
                'NIC.1#DNSRacName': self.args.host,
                'NICStatic.1#DNSDomainFromDHCP': 'Disabled',
                'NICStatic.1#DNSDomainName': f'mgmt.{self.netbox_data["site"]["slug"]}.wmnet',
            },
            'System.Embedded.1': {
                'ServerPwr.1#PSRapidOn': 'Disabled',
            }
        }

        self.config_changes_idrac10 = {
            'BIOS.Setup.1-1': {
                'CpuInterconnectBusLinkPower': 'Enabled',
                'EnergyPerformanceBias': 'BalancedPerformance',
                'PcieAspmL1': 'Enabled',
                'ProcC1E': 'Enabled',
                'ProcCStates': 'Enabled',
                'ProcPwrPerf': 'OsDbpm',
                'SysProfile': 'PerfPerWattOptimizedOs',
                'UncoreFrequency': 'DynamicUFS',
                'UsbPorts': 'OnlyBackPortsOn',
                'HttpDev1TlsMode': 'None',
            },
            'iDRAC.Embedded.1': {
                'IPMILan.1#Enable': 'Enabled',
                'IPv4.1#DHCPEnable': 'Disabled',
                'IPv4.1#StaticAddress': str(self.redfish.interface.ip),
                'IPv4.1#StaticDNS1': DNS_ADDRESS,
                'IPv4.1#StaticGateway': str(next(iter(self.redfish.interface.network.hosts()))),
                'IPv4.1#StaticNetmask': str(self.redfish.interface.netmask),
                'Network.1#DNSRacName': self.args.host,
                'Network.1#DNSDomainNameFromDHCP': 'Disabled',
                'Network.1#StaticDNSDomainName': f'mgmt.{self.netbox_data["site"]["slug"]}.wmnet',
            },
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
        super().run()

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
                reboot_chassis(self.vendor, self.redfish)

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

    def _config_host(self):  # pylint: disable=too-many-branches
        """Provision the BIOS and iDRAC settings."""
        if self.redfish.hw_model >= 10:
            logger.info("Using iDRAC 10 config changes.")
            self.config_changes = self.config_changes_idrac10

        config = self._get_config()
        if config.model.lower() in OLD_SERIAL_MODELS:
            self.config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedirCom2'
            self.config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Serial1Com1Serial2Com2'
            self.config_changes['BIOS.Setup.1-1']['InternalUsb'] = 'Off'
        else:
            self.config_changes['BIOS.Setup.1-1']['SerialComm'] = 'OnConRedir'
            self.config_changes['BIOS.Setup.1-1']['SerialPortAddress'] = 'Com2'

        if self.uefi:
            self.config_changes['BIOS.Setup.1-1']['HttpDev1EnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1DhcpEnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1DnsDhcpEnDis'] = 'Enabled'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1Protocol'] = 'IPv4'
            self.config_changes['BIOS.Setup.1-1']['HttpDev1VlanEnDis'] = 'Disabled'

        if 'IntelSgx' in config.components['BIOS.Setup.1-1']:
            self.config_changes['BIOS.Setup.1-1']['IntelSgx'] = 'Off'

        if 'BiosNvmeDriver' in config.components['BIOS.Setup.1-1']:
            self.config_changes['BIOS.Setup.1-1']['BiosNvmeDriver'] = 'AllDrives'

        if 'WebServer.1#HostHeaderCheck' in config.components['iDRAC.Embedded.1']:
            self.config_changes['iDRAC.Embedded.1']['WebServer.1#HostHeaderCheck'] = 'Disabled'

        if self.redfish.hw_model >= 10:
            # In single CPU systems running on IDRAC 10 some options
            # may not be tunable (for example, cpu virtualization enabled by default)
            # or present at all (like ProcX2Apic that is only available on multi-CPU systems)
            # UEFI seems also sometimes the only available BootMode.
            if 'BootMode' in config.components['BIOS.Setup.1-1']:
                self.config_changes['BIOS.Setup.1-1']['BootMode'] = 'Uefi' if self.uefi else 'Bios'
            else:
                logger.info('Skipping BootMode config in the BIOS, not available.')
            if 'ProcVirtualization' in config.components['BIOS.Setup.1-1']:
                self.config_changes['BIOS.Setup.1-1']['ProcVirtualization'] = \
                    'Enabled' if self.args.enable_virtualization else 'Disabled'
            else:
                logger.info('Skipping ProcVirtualization config in the BIOS, not available.')
            if 'ProcX2Apic' in config.components['BIOS.Setup.1-1']:
                self.config_changes['BIOS.Setup.1-1']['ProcX2Apic'] = 'Disabled'
            else:
                logger.info('Skipping ProcX2Apic config in the BIOS, not available.')
            # This option seems not present in modern IDRAC 10 hosts, but we have only
            # tested single CPU ones so far.
            # More info: T392851
            if 'ServerPwr.1#PSRapidOn' in config.components['System.Embedded.1']:
                self.config_changes['System.Embedded.1']['ServerPwr.1#PSRapidOn'] = 'Disabled'
            else:
                logger.info('Skipping ServerPwr.1#PSRapidOn config in System.Embedded.1, not available.')

        self._config_pxe(config)
        self._disable_lldp(config)
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

    def _all_nics(self, config) -> list:
        """Return a list of all the server's NIC in a "NIC." format.

        Arguments:
            config (spicerack.redfish.RedfishDellSCP): the configuration to modify.

        Returns:
            list: The list of NICs.

        Raises:
            RuntimeError: if unable to find any NIC.

        """
        if self.all_nics:
            return self.all_nics
        all_nics = sorted(key for key in config.components.keys() if key.startswith('NIC.'))
        if not all_nics:
            if self.redfish.hw_model >= 10:
                nics_json = self.redfish.request(
                    'GET', f'{self.redfish.system_manager}/EthernetInterfaces').json()
                for nic_json in nics_json['Members']:
                    all_nics.append(nic_json['@odata.id'].split('/')[-1])
            else:
                raise RuntimeError('Unable to find any NIC.')
        self.all_nics = all_nics
        return all_nics

    def _config_pxe(self, config):  # pylint: disable=too-many-branches
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
        all_nics = self._all_nics(config)
        pxe_nic = self._get_pxe_nic(all_nics)
        if self.uefi:
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
        # We do it only for Legacy since in UEFI the debian installer takes
        # care of setting the right first-boot option.
        # Corner cases like T406964 are also handled/solved simply not
        # touching the boot config.
        if not self.uefi:
            new_boot_order = ['HardDisk.List.1-1', pxe_nic]
            # SetBootOrderEn defaults to comma-separated, but some hosts might differ
            separator = ', ' if ', ' in config.components['BIOS.Setup.1-1']['SetBootOrderEn'] else ','
            self.config_changes['BIOS.Setup.1-1']['SetBootOrderEn'] = separator.join(new_boot_order)
            # BiosBootSeq defaults to comma-space-separated, but some hosts might differ
            # Use a default if the host is in UEFI mode and dosn't have the setting at all.
            bios_boot_seq = config.components['BIOS.Setup.1-1'].get('BiosBootSeq', ', ')
            separator = ',' if ',' in bios_boot_seq and ', ' not in bios_boot_seq else ', '
            self.config_changes['BIOS.Setup.1-1']['BiosBootSeq'] = separator.join(new_boot_order)

    def _disable_lldp(self, config):
        """Disable LLDP embeded on Broadcom NICs as they conflict with the OS LLDP deamon.

        Arguments:
            config (spicerack.redfish.RedfishDellSCP): the configuration to modify.

        """
        all_nics = self._all_nics(config)
        nic = self._get_pxe_nic(all_nics)
        for attribute in ('Broadcom_LLDPNearestBridge', 'Broadcom_LLDPNearestNonTPMRBridge'):
            if config.components[nic].get(attribute, '') == 'Enabled':
                self.config_changes[nic][attribute] = 'Disabled'
                logger.info('Disabled LLDP on nic %s, attribute %s', nic, attribute)
