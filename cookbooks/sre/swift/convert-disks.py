"""Convert the Disks of the host from single PV to non-RAID disks."""
import logging

from datetime import timedelta
from pprint import pformat

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.hosts import OS_VERSIONS

logger = logging.getLogger(__name__)


class ConvertDisks(CookbookBase):
    """Convert the Disks of the host from single PV to non-RAID disks.

    Actions performed:
        * Downtime the server on Icinga/Alertmanager.
        * Upgrade the iDRAC firmware
        * Find all the disks present in the RAID controller as Virtual disks.
        * Delete the Virtual disks.
        * Convert the disks from single-disk Virtual disks to non-RAID disks.
        * Set boot device in the RAID controller to the first Virtual disk (temporary workaround).
        * Reimage the server

    Usage:
        cookbook sre.swift.convert-ssds --os bookworm example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--no-firmware-upgrade', action='store_true',
                            help='skip firmware upgrade & reboot step')
        parser.add_argument('--os', choices=OS_VERSIONS, required=True,
                            help='the Debian version to install. Mandatory parameter. One of %(choices)s.')
        parser.add_argument(
            'host', help='Short hostname of the host to provision, not FQDN'
        )

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ConvertDisksRunner(args, self.spicerack)


class ConvertDisksRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the provision runner."""
        ensure_shell_is_durable()
        self.host = args.host
        self.args = args
        self.spicerack = spicerack

        netbox_server = spicerack.netbox_server(self.host)
        netbox_data = netbox_server.as_dict()
        query = f"P{{{netbox_server.fqdn}}} and (A:swift or A:thanos or P{{O:insetup::data_persistence}})"
        self.remote_host = spicerack.remote().query(query)
        if len(self.remote_host) != 1:
            raise RuntimeError(f'Host lookup returned {len(self.remote_host)} hosts instead of 1 from query: {query}')
        self.puppet_host = spicerack.puppet(self.remote_host)
        self._raid_storage_uri = '/redfish/v1/Systems/System.Embedded.1/Storage/RAID.Integrated.1-1'
        self._raid_action_uri = '/redfish/v1/Systems/System.Embedded.1/Oem/Dell/DellRaidService/Actions/DellRaidService'
        self.reason = spicerack.admin_reason('Converting Disks to non-RAID')
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_host.hosts)

        if netbox_data['device_type']['manufacturer']['slug'] != 'dell':
            vendor = netbox_data['device_type']['manufacturer']['name']
            raise RuntimeError(
                f'Host {self.host} manufacturer is {vendor}. Only Dell is supported.'
            )

        self.redfish = spicerack.redfish(self.host)
        self.redfish.check_connection()

        raid_storage = self.redfish.request('get', self._raid_storage_uri).json()
        self.storage_controller_fqdd = raid_storage['@odata.id'].split('/')[-1]
        self.pd_array = []
        self.virtual_disks = set()
        for drive in raid_storage['Drives']:
            drive_uri = drive['@odata.id']
            drive_fqdd = drive_uri.split("/")[-1]
            drive_data = self.redfish.request('get', drive_uri).json()
            if len(drive_data['Links']['Volumes']) != 1:
                logger.warning(
                    'Skipping drive %s, expected 1 linked volumes got %d: %s',
                    drive_fqdd,
                    len(drive_data['Links']['Volumes']),
                    drive_data['Links']['Volumes'],
                )
                continue

            linked_volume = drive_data['Links']['Volumes'][0]['@odata.id']
            if not linked_volume.split("/")[-1].startswith('Disk.Virtual'):
                logger.info(
                    'Skipping non virtual volume %s for drive %s',
                    linked_volume,
                    drive_fqdd,
                )
                continue

            self.pd_array.append(drive_fqdd)
            self.virtual_disks.add(linked_volume)

        logger.info(
            'Found %d Physical disks to convert to non-RAID: %s',
            len(self.pd_array),
            self.pd_array,
        )
        logger.info(
            'Found %d Virtual disks to delete: %s',
            len(self.virtual_disks),
            self.virtual_disks,
        )

        if not self.pd_array:
            raise RuntimeError('Nothing to do')

        ask_confirmation(
            f'Are you sure to proceed with the above changes? {self.runtime_description}?'
        )

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.host}'

    def run(self):
        """Run the cookbook."""
        with self.alerting_hosts.downtimed(self.reason, duration=timedelta(hours=2)):
            if self.args.no_firmware_upgrade:
                logger.info('Skipping firmware upgrade as requested')
            else:
                logger.info('upgrading idrac: %s', self.host)
                self.spicerack.run_cookbook(
                    'sre.hardware.upgrade-firmware',
                    ['-c', 'idrac', str(self.remote_host)],
                    raises=True)
                # Force a reboot, as otherwise the subsequent drive deletion
                # jobs don't work
                logger.info('Rebooting host: %s', self.remote_host)
                self.spicerack.run_cookbook(
                    'sre.hosts.reboot-single',
                    [str(self.remote_host), "--reason", "idrac upgrade"],
                    raises=True)

            logger.info('Unmount disks: %s', self.host)
            # Stop any running swift processes
            self.remote_host.run_async('systemctl stop swift*')
            # Ignore the error code we are going to wipe the disks anyway so its not a big issue if we cause an issue
            self.remote_host.run_async('find  /srv/swift-storage/ -type d -maxdepth  1 -exec umount {} + || true')
            self._convert()
            logger.info('Disk conversion done; now reimaging %s', self.host)
            self.spicerack.run_cookbook('sre.hosts.reimage', ['--os', self.args.os, self.host], raises=True)

    def _convert(self):
        """Perform the conversion of the disks."""
        for virtual_disk in self.virtual_disks:
            logger.info('Deleting Virtual disk %s', virtual_disk)
            results = self.redfish.poll_task(
                self.redfish.submit_task(virtual_disk, method='delete')
            )
            logger.info(pformat(results))

        logger.info('Converting Physical disks to non-RAID: %s', self.pd_array)
        job_url = self.redfish.submit_task(
            f'{self._raid_action_uri}.ConvertToNonRAID',
            data={'PDArray': self.pd_array},
        )
        # We get returned a Job, but we want a Task cf. T357764
        logger.debug('Returned job URL: %s', job_url)
        jid = job_url.split('/')[-1]
        task_url = "/redfish/v1/TaskService/Tasks/%s" % jid
        logger.debug('Using task URL: %s', task_url)
        results = self.redfish.poll_task(task_url)
        logger.info(pformat(results))
