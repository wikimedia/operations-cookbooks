"""Convert the SSDs of the host from single PV to non-RAID disks."""
import logging

from datetime import datetime, timedelta
from pprint import pformat

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.redfish import ChassisResetPolicy
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable


logger = logging.getLogger(__name__)


class ConvertSSDs(CookbookBase):
    """Convert the SSDs of the host from single PV to non-RAID disks.

    Actions performed:
        * Downtime the server on Icinga/Alertmanager.
        * Power off the server gracefully and wait until it's powered off.
        * Find all the SSD disks present in the RAID controller as Virtual disks.
        * Delete the Virtual disks.
        * Convert the disks from single-disk Virtual disks to non-RAID disks.
        * Set the boot device in the RAID controller to the first Virtual disk (temporary workaround).
        * Power on the server.

    Usage:
        cookbook sre.swift.convert-ssds example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('host', help='Short hostname of the host to provision, not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ConvertSSDsRunner(args, self.spicerack)


class ConvertSSDsRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.host = args.host

        netbox_server = spicerack.netbox_server(self.host)
        netbox_data = netbox_server.as_dict()
        self.fqdn = netbox_server.mgmt_fqdn
        self.ipmi = spicerack.ipmi(self.fqdn)
        self.remote_host = spicerack.remote().query(netbox_server.fqdn)
        self.puppet_host = spicerack.puppet(self.remote_host)
        self.reason = spicerack.admin_reason('Converting SSDs to non-RAID')
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)

        if netbox_data['device_type']['manufacturer']['slug'] != 'dell':
            vendor = netbox_data['device_type']['manufacturer']['name']
            raise RuntimeError(f'Host {self.host} manufacturer is {vendor}. Only Dell is supported.')

        self.redfish = spicerack.redfish(netbox_server.mgmt_fqdn, 'root')
        self.redfish.check_connection()

        storage = self.redfish.request('get', '/redfish/v1/Systems/System.Embedded.1/Storage/').json()
        raid_storages = []
        for storage_member in storage['Members']:
            if storage_member['@odata.id'].split('/')[-1].startswith('RAID'):
                raid_storages.append(storage_member['@odata.id'])

        if len(raid_storages) != 1:
            raise RuntimeError(f'Expected 1 RAID storage, found: {raid_storages}')

        raid_storage = self.redfish.request('get', raid_storages[0]).json()
        self.storage_controller_fqdd = raid_storage['@odata.id'].split('/')[-1]
        self.pd_array = []
        self.virtual_disks = set()
        for drive in raid_storage['Drives']:
            drive_uri = drive['@odata.id']
            drive_fqdd = drive_uri.split("/")[-1]
            drive_data = self.redfish.request('get', drive_uri).json()
            if drive_data['MediaType'] != 'SSD':
                logger.debug('Skipping non SSD drive %s: %s', drive_fqdd, drive_data['MediaType'])
                continue

            if len(drive_data['Links']['Volumes']) != 1:
                logger.warning('Skipping SSD drive %s, expected 1 linked volumes got %d: %s',
                               drive_fqdd, len(drive_data['Links']['Volumes']), drive_data['Links']['Volumes'])
                continue

            linked_volume = drive_data['Links']['Volumes'][0]['@odata.id']
            if not linked_volume.split("/")[-1].startswith('Disk.Virtual'):
                logger.info('Skipping non virtual volume %s for SSD drive %s', linked_volume, drive_fqdd)
                continue

            self.pd_array.append(drive_fqdd)
            self.virtual_disks.add(linked_volume)

        logger.info('Found %d Physical disks to convert to non-RAID: %s', len(self.pd_array), self.pd_array)
        logger.info('Found %d Virtual disks to delete: %s', len(self.virtual_disks), self.virtual_disks)

        if not self.pd_array:
            raise RuntimeError('Nothing to do')

        ask_confirmation(f'Are you sure to proceed with the above changes? {self.runtime_description}?')

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.host}'

    def run(self):
        """Run the cookbook."""
        with self.alerting_hosts.downtimed(self.reason, duration=timedelta(hours=2)):
            logger.info('Powering off %s', self.host)
            self.redfish.chassis_reset(ChassisResetPolicy.GRACEFUL_SHUTDOWN)
            self._wait_power_off()

            self._convert()

            logger.info('Powering on %s', self.host)
            startup_time = datetime.now()
            self.redfish.chassis_reset(ChassisResetPolicy.ON)
            self.remote_host.wait_reboot_since(startup_time, print_progress_bars=False)
            self.puppet_host.wait_since(startup_time)

    @retry(tries=60, delay=timedelta(seconds=20), backoff_mode='constant', exceptions=(RuntimeError,),
           failure_message='Host power is not yet off')
    def _wait_power_off(self):
        """Poll until the power is off."""
        state = self.redfish.get_power_state()
        if state.lower() != 'off':
            raise RuntimeError(f'Host power state is not yet Off: {state}')

    def _convert(self):
        """Perform the conversion of the disks."""
        for virtual_disk in self.virtual_disks:
            logger.info('Deleting Virtual disk %s', virtual_disk)
            results = self.redfish.poll_task(self.redfish.submit_task(virtual_disk, method='delete'))
            logger.info(pformat(results))

        logger.info('Converting Physical disks to non-RAID: %s', self.pd_array)
        results = self.redfish.poll_task(self.redfish.submit_task(
            '/redfish/v1/Systems/System.Embedded.1/Oem/Dell/DellRaidService/Actions/DellRaidService.ConvertToNonRAID',
            data={'PDArray': self.pd_array},
        ))
        logger.info(pformat(results))

        # Temporary workaround: use the first Virtual disk as boot device in the RAID controller
        volumes = self.redfish.request(
            'get', '/redfish/v1/Systems/System.Embedded.1/Storage/RAID.Integrated.1-1/Volumes').json()
        for volume in volumes:
            virtual_disk_fqdd = volume.split('/')[-1]
            if virtual_disk_fqdd.startswith('Disk.Virtual'):
                logging.info('Using %s as boot device for the RAID controller', virtual_disk_fqdd)
                break
        else:
            raise RuntimeError('Unable to find a virtual disk to set as boot device')

        results = self.redfish.poll_task(self.redfish.submit_task(
            '/redfish/v1/Systems/System.Embedded.1/Oem/Dell/DellRaidService/Actions/DellRaidService.SetBootVD',
            data={
                'ControllerFQDD': self.storage_controller_fqdd,
                'VirtualDiskFQDD': virtual_disk_fqdd,
            }
        ))
        logger.info(pformat(results))
