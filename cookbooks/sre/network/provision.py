"""Zero-Touch provisioning of network devices."""
import ipaddress
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.dhcp import DHCPConfMgmt
from spicerack.exceptions import SpicerackError
from spicerack.remote import RemoteError
from wmflib.interactive import confirm_on_failure, ensure_shell_is_durable


PROVISION_ALLOWED_STATUSES = ('planned',)
PROVISION_ALLOWED_ROLES = ('cloudsw', 'asw')
logger = logging.getLogger(__name__)


class Provision(CookbookBase):
    """Provision a new network device using the Zero-Touch Provisioning method (ZTP).

    Usage:
        cookbook sre.network.provision lsw1-a1-codfw

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('device', help='Short hostname of the device to provision as defined in Netbox.')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return ProvisionRunner(args, self.spicerack)


class ProvisionRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the DHCP runner."""
        ensure_shell_is_durable()
        self.device = args.device
        self.run_cookbook = spicerack.run_cookbook

        self.netbox = spicerack.netbox(read_write=True)
        self.netbox_device = self._get_netbox_device()
        if self.netbox_device.device_role.slug not in PROVISION_ALLOWED_ROLES:
            raise RuntimeError(f'Device {self.device} has role {self.netbox_device.role.slug}, expected one of '
                               f'{PROVISION_ALLOWED_ROLES}')

        if self.netbox_device.status.value not in PROVISION_ALLOWED_STATUSES:
            raise RuntimeError(f'Device {self.device} has status {self.netbox_device.status}, expected one of '
                               f'{PROVISION_ALLOWED_STATUSES}')

        if self.netbox_device.primary_ip:
            raise RuntimeError(
                f'Device {self.device} has already a primary IP {self.netbox_device.primary_ip}, bailing out')

        self.fqdn = f'{self.netbox_device.name}.mgmt.{self.netbox_device.site.slug}.wmnet'
        self.remote = spicerack.remote()
        # DHCP automation
        try:
            self.dhcp_hosts = self.remote.query(f'A:installserver and A:{self.netbox_device.site.slug}')
        except RemoteError:  # Fallback to eqiad's install server if the above fails, i.e. for a new DC
            self.dhcp_hosts = self.remote.query('A:installserver and A:eqiad')

        self.dhcp = spicerack.dhcp(self.dhcp_hosts)
        self.ip_address = None
        self.rollback_ip = False
        self.rollback_dns = False

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for device {self.fqdn}'

    def rollback(self):
        """Called by Spicerack in case of failure."""
        if self.rollback_ip and self.ip_address is not None:
            logger.info('Rolling back IP creation, deleting IP %s', self.ip_address)
            if not self.ip_address.delete():
                logger.error('Failed to delete address %s, manual intervention required', self.ip_address)

        if self.rollback_dns:
            self._propagate_dns('Remove')

    def _get_netbox_device(self):
        """Return the Netbox device for the current name. Used also to refresh the object."""
        return self.netbox.api.dcim.devices.get(name=self.device)

    def _get_dhcp_config(self):
        """Get the DHCP config snippet."""
        return DHCPConfMgmt(
            datacenter=self.netbox_device.site.slug,
            serial=self.netbox_device.serial,
            manufacturer=self.netbox_device.device_type.manufacturer.slug,
            fqdn=self.fqdn,
            ipv4=ipaddress.IPv4Interface(self.netbox_device.primary_ip4).ip,
        )

    def _propagate_dns(self, prefix):
        """Propagate the DNS changes."""
        def run_raise(name, args):
            ret = self.run_cookbook(name, args)
            if ret:
                raise RuntimeError(f'Failed to run cookbook {name}')

        confirm_on_failure(run_raise, 'sre.dns.netbox', [f'{prefix} management record for {self.netbox_device}'])
        self.rollback_dns = True

    def _allocate_ip(self):
        """Allocate the management IP in Netbox, set its DNS name and assign it to the correct interface."""
        interface = self.netbox.api.dcim.interfaces.get(device_id=self.netbox_device.id, mgmt_only=True, enabled=True)
        if interface is None:
            raise RuntimeError(f'Unable to find mgmt_only enabled interface for device {self.netbox_device}')

        prefix = self.netbox.api.ipam.prefixes.get(
            status='active',
            family=4,
            role='management',
            site=self.netbox_device.site.slug,
            tenant_id='null')
        self.ip_address = prefix.available_ips.create()
        if self.ip_address is None:
            raise RuntimeError(f'Failed to allocate an IP in prefix {prefix}')

        self.ip_address.dns_name = self.fqdn
        self.ip_address.assigned_object_id = interface.id
        self.ip_address.assigned_object_type = 'dcim.interface'
        logger.info('Allocating IP %s with DNS name %s in prefix %s and attached it to the interface %s of device %s '
                    'marking it as primary IPv4', self.ip_address, self.fqdn, prefix, interface, self.netbox_device)
        if not self.ip_address.save():
            raise RuntimeError(f'Failed to save IP {self.ip_address}')

        self.netbox_device.primary_ip4 = self.ip_address
        if not self.netbox_device.save():
            raise RuntimeError(f'Failed to set IP {self.ip_address} as primary for {self.netbox_device}')

        self.rollback_ip = True
        # Refresh the netbox device
        self.netbox_device = self._get_netbox_device()

    @retry(tries=20, backoff_mode='linear', failure_message='Device still not reachable, keep polling')
    def _poll_device(self, remote_device):
        """Poll the device via SSH until its reachable with the Homer's key."""
        command = 'show system uptime local'
        results = remote_device.run_sync(command, is_safe=True)
        for _, output in results:
            message = output.message().decode()
            if 'booted' in message:
                break

            raise SpicerackError(f'Command "{command}" does not have "booted" in it, got: {message}')
        else:
            raise SpicerackError(f'Command "{command}" did not return any output')

    def run(self):
        """Run the cookbook."""
        self._allocate_ip()
        self._propagate_dns('Add')
        remote_device = self.remote.query(f'D{{{self.fqdn}}}')

        with self.dhcp.config(self._get_dhcp_config()):
            confirm_on_failure(self._poll_device, remote_device)
            logger.info('Device %s is now reachable', self.fqdn)
            # TODO: run homer on self.fqdn
