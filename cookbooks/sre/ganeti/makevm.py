"""Create a new Virtual Machine in Ganeti"""

import argparse
import logging
import re

from ipaddress import ip_interface

from wmflib.constants import CORE_DATACENTERS, DATACENTER_NUMBERING_PREFIX
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.ganeti import INSTANCE_LINKS, STORAGE_TYPES

from cookbooks.sre.ganeti import add_location_args, set_default_group
from cookbooks.sre.hosts import OS_VERSIONS


logger = logging.getLogger(__name__)
PRIMARY_INTERFACE_NAME = '##PRIMARY##'
PER_RACK_VLAN_DATACENTERS = ('drmrs', 'esams', 'magru')


class GanetiMakeVM(CookbookBase):
    """Create a new Virtual Machine in Ganeti

    * Pre-allocate the primary IPs and set their DNS name
    * Update the DNS records
    * Create the VM on Ganeti
    * Force a sync of Ganeti VMs to Netbox in the same DC
    * Update Netbox attaching the pre-allocated IPs to the host's primary interface

    Examples:
        Create a Ganeti VM vmname.codfw.wmnet in the codfw Ganeti cluster
        on group B with 1 vCPUs, 3GB of RAM, 100GB of disk in the private network:

            makevm --vcpus 1 --memory 3 --disk 100 --cluster codfw --group B vmhostname
            makevm --vcpus 1 --memory 3 --disk 100 --cluster eqsin vmhostname

    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        def positive_int(param):
            """Type validator for argparse that accepts only positive integers."""
            value = int(param)
            if value <= 0:
                raise argparse.ArgumentTypeError(f'{param} is not a positive integer')
            return value

        def validate_memory(param):
            """Type validator for argparse that accepts only positive floats greater or equal to 1.5."""
            param = float(param)
            if param <= 1.5:
                raise argparse.ArgumentTypeError('Memory must be at least 1.5G')
            return param

        def validate_hostname(param):
            """Helper to instruct people to pass in a hostname instead of fqdn"""
            if '.' in param:
                raise argparse.ArgumentTypeError('This cookbook now takes a hostname, not a fqdn')
            return param

        parser = super().argument_parser()
        parser.add_argument('--skip-v6', action='store_true', help='To skip the generation of the IPv6 DNS record.')
        parser.add_argument(
            '--vcpus', type=positive_int, default=1, help='The number of virtual CPUs to assign to the VM.')
        parser.add_argument(
            '--memory', type=validate_memory, default=1.5,
            help='The amount of RAM to allocate to the VM in GB. ints and floats values are allowed'
        )
        parser.add_argument(
            '--disk', type=positive_int, default=10, help='The amount of disk to allocate to the VM in GB.')
        parser.add_argument('--network', choices=INSTANCE_LINKS, default='private',
                            help='Specify the type of network to assign to the VM.')
        parser.add_argument('--storage_type', choices=STORAGE_TYPES, default='drbd',
                            help='the storage type of the VM. One of %(choices)s, default to drbd')
        parser.add_argument('--os', choices=OS_VERSIONS + ('none',), required=True,
                            help='the Debian version to install. One of %(choices)s, use "none" to skip installation')
        parser.add_argument('-t', '--task-id', help='the Phabricator task ID to update and refer (i.e.: T12345)')
        parser.add_argument('-p', '--puppet-version', choices=(5, 7), default=7, type=int,
                            help='The puppet version to use when reimaging. One of %(choices)s.')
        add_location_args(parser)
        parser.add_argument('hostname', type=validate_hostname, help='The hostname for the VM (not the FQDN).')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        set_default_group(self.spicerack.netbox(), args)
        return GanetiMakeVMRunner(args, self.spicerack)


class GanetiMakeVMRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """Create a new Virtual Machine in Ganeti runner"""

    def __init__(self, args, spicerack):
        """Create a new Virtual Machine in Ganeti."""
        self.args = args
        self.ganeti = spicerack.ganeti()
        self.group = self.ganeti.get_group(args.group, cluster=args.cluster)
        self.routed = self.group.cluster.routed
        self.cluster = args.cluster
        self.hostname = args.hostname
        self.vcpus = args.vcpus
        self.memory = args.memory
        self.network = args.network
        self.disk = args.disk
        self.storage_type = args.storage_type
        self.skip_v6 = args.skip_v6
        self.spicerack = spicerack
        self.netbox = self.spicerack.netbox(read_write=True)
        self.fqdn = make_fqdn(self.hostname, self.network, self.group.site)
        self.allocated = []  # Store allocated IPs to rollback them on failure
        self.dns_propagated = False  # Whether to run the DNS cookbook on rollback
        self.need_netbox_sync = False  # Whether to sync the VM to Netbox on rollback
        self.skip_rollback = False  # Whether to skip the rollback actions because the VM has been created

        print('Ready to create Ganeti VM {a.fqdn} in the {a.cluster} cluster on group {a.group.name} with {a.vcpus} '
              'vCPUs, {a.memory}GB of RAM, {a.disk}GB of disk in the {a.network} network.'.format(a=self))
        ask_confirmation('Is this correct?')

        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for new host {self.fqdn}'

    def rollback(self):
        """Rollback IP and DNS assignments on failure."""
        if self.skip_rollback:
            logger.warning('The VM %s has been fully created, not performing rollback. If the reimage failed just '
                           'run the reimage cookbook for it.', self.fqdn)
            return

        for address in self.allocated:
            ip = self.netbox.api.ipam.ip_addresses.get(address=address)
            logger.info('Deleting assigned IP %s', ip)
            ip.delete()

        if self.dns_propagated:
            self._propagate_dns('Remove')

        if self.need_netbox_sync:
            self._ganeti_netbox_sync()

    def _propagate_dns(self, prefix):
        """Run the sre.dns.netbox cookbook to propagate the DNS records."""
        self.spicerack.run_cookbook('sre.dns.netbox', [f'{prefix} records for VM {self.fqdn}'], confirm=True)
        self.dns_propagated = True
        # Clean out DNS cache to remove stale NXDOMAINs
        self.spicerack.run_cookbook('sre.dns.wipe-cache', [self.fqdn], confirm=True)

    def _ganeti_netbox_sync(self):
        """Perform a sync from Ganeti to Netbox in the affected DC."""
        cluster = self.group.cluster.name
        logger.info('Syncing VMs in group %s of cluster %s to Netbox', self.group.name, cluster)
        self.spicerack.netbox_master_host().run_sync(
            f'systemctl start netbox_ganeti_{cluster}_sync.service')
        self.need_netbox_sync = False

    def _v4_mapped_v6_ip(self, prefix_v6, ip_v4) -> str:
        """Generate the IPv6 address embedding the IPv4 address."""
        # For example from an IPv4 address 10.0.0.1 and an IPv6 prefix 2001:db8:3c4d:15::/64
        # the mapped IPv6 address 2001:db8:3c4d:15:10:0:0:1/64 is generated.
        prefix_v6_base, prefix_v6_mask = str(prefix_v6).split("/")
        mapped_v4 = str(ip_v4).split('/', maxsplit=1)[0].replace(".", ":")
        prefix_v6 = prefix_v6_base.rstrip(':')
        if self.routed:
            prefix_v6_mask = '128'
        return f'{prefix_v6}:{mapped_v4}/{prefix_v6_mask}'

    def _find_switched_prefixes(self) -> tuple:
        """Fetch the v4 and v6 prefixes matching the hosts site and vlan type."""
        # Pre-allocate IPs
        # TODO: simplify the VLAN detection logic
        if self.group.site in CORE_DATACENTERS or self.group.site in PER_RACK_VLAN_DATACENTERS:
            location = self.group.name.lower().split('_')[-1].replace('-test', '')
            vlan_name = f'{self.network}1-{location}-{self.group.site}'
        else:
            vlan_name = f'{self.network}1-{self.group.site}'

        vlan = self.netbox.api.ipam.vlans.get(name=vlan_name, status='active')
        if not vlan:
            raise RuntimeError(f'Failed to find VLAN with name {vlan_name}')

        prefix_v4 = self.netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=4)
        prefix_v6 = self.netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=6)
        return (prefix_v4, prefix_v6)

    def _find_routed_prefixes(self) -> tuple:
        """Fetch the v4 and v6 prefixes matching the host site and network type for routed Ganeti."""
        filters = {'status': 'active',
                   'role': 'virtual-machines',
                   'site': self.group.site,
                   'description__isw': self.network}  # A bit brittle
        try:
            prefix_v4 = self.netbox.api.ipam.prefixes.get(family=4, **filters)
            prefix_v6 = self.netbox.api.ipam.prefixes.get(family=6, **filters)
        except ValueError as e:
            raise RuntimeError(('More than 1 possible prefix found to allocate IPs.')) from e
        return (prefix_v4, prefix_v6)

    def run(self):  # pylint: disable=too-many-statements
        """Create a new Ganeti VM as specified."""
        if self.routed:
            prefix_v4, prefix_v6 = self._find_routed_prefixes()
        else:
            prefix_v4, prefix_v6 = self._find_switched_prefixes()
        if not prefix_v4 or not prefix_v6:
            raise RuntimeError('No IPv4 or v6 prefix found to allocate IPs to this VM.')
        ip_v4_data = prefix_v4.available_ips.create({})
        ip_v4 = self.netbox.api.ipam.ip_addresses.get(address=ip_v4_data['address'])
        if self.routed:
            # by default Netbox creates IPs with the same prefix than the subnet, but we need a /32s v4
            ip_v4.address = f"{str(ip_v4.address).split('/', maxsplit=1)[0]}/32"
        ip_v4.dns_name = self.fqdn
        if not ip_v4.save():
            raise RuntimeError(f'Failed to save DNS name for IP {ip_v4} on Netbox')
        logger.info('Allocated IPv4 %s', ip_v4.address)
        self.allocated.append(ip_v4.address)
        logger.info('Set DNS name of IP %s to %s', ip_v4, self.fqdn)

        ipv6_address = self._v4_mapped_v6_ip(prefix_v6, ip_v4)
        if self.skip_v6:
            dns_name_v6 = ''
        else:
            dns_name_v6 = self.fqdn
        ip_v6 = self.netbox.api.ipam.ip_addresses.create(address=ipv6_address, status='active', dns_name=dns_name_v6)
        self.allocated.append(ip_v6.address)
        logger.info('Allocated IPv6 %s with DNS name %s', ip_v6, dns_name_v6)

        self._propagate_dns('Add')

        # Create the VM
        instance = self.ganeti.instance(self.fqdn, cluster=self.cluster)

        logger.info('The Ganeti\'s command output will be printed at the end.')

        self.need_netbox_sync = True
        net = ip_interface(ip_v4.address).ip if self.routed else self.network
        ip6 = ip_interface(ip_v6.address).ip
        instance.add(group=self.group.name, vcpus=self.vcpus, memory=self.memory, storage_type=self.storage_type,
                     disk=self.disk, net=net, ip6=ip6)

        self._ganeti_netbox_sync()

        # Get the synced VM
        @retry(tries=30, backoff_mode='linear', exceptions=(RuntimeError,))
        def get_vm(netbox):
            vm = netbox.api.virtualization.virtual_machines.get(name=self.hostname)
            if not vm:
                raise RuntimeError(f'VM {self.hostname} not yet found on Netbox')

            return vm

        # Update Netbox
        vm = get_vm(self.netbox)
        iface = self.netbox.api.virtualization.interfaces.create(
            virtual_machine=vm.id, name=PRIMARY_INTERFACE_NAME, type='virtual')
        logger.info('Created interface %s on VM %s', PRIMARY_INTERFACE_NAME, vm)

        ip_v4.assigned_object_id = iface.id
        ip_v4.assigned_object_type = 'virtualization.vminterface'
        if not ip_v4.save():
            raise RuntimeError(f'Failed to attach IPv4 {ip_v4} to interface {iface}')

        ip_v6.assigned_object_id = iface.id
        ip_v6.assigned_object_type = 'virtualization.vminterface'
        if not ip_v6.save():
            raise RuntimeError(f'Failed to attach IPv6 {ip_v6} to interface {iface}')

        vm.primary_ip4 = ip_v4
        vm.primary_ip6 = ip_v6
        if not vm.save():
            raise RuntimeError(f'Failed to set primary IPv4/6 to VM {vm}')

        logger.info(
            'Attached IPv4 %s and IPv6 %s to VM %s and marked as primary IPs',
            ip_v4, ip_v6, vm)

        # Update Puppet hiera data from NetBox.
        hiera_ret = self.spicerack.run_cookbook('sre.puppet.sync-netbox-hiera',
                                                [f'Triggered by {__name__}: created new VM {self.fqdn}'])

        if hiera_ret:
            raise RuntimeError('Failed to update Puppet with NetBox data for VM: {vm}')

        # The VM is now created, no need to rollback anything.
        self.skip_rollback = True

        # No OS is specified, do not reimage.
        if self.args.os == 'none':
            logger.info('No operating system specified, no reimaging required or VM: %s', vm)
            return hiera_ret

        # Configure parameters for reimaging cookbook.
        params = ['--new', '--os', self.args.os, '--puppet-version', str(self.args.puppet_version)]
        if self.args.task_id:
            params.extend(['--task-id', self.args.task_id])
        params.append(self.hostname)

        return self.spicerack.run_cookbook('sre.hosts.reimage', params)


def make_fqdn(hostname: str, network: str, datacenter: str) -> str:
    """Create a fqdn based on the hostname, network and datacenter"""
    # Validate that the hostname uses the correct number for the datacenter
    # Note that misc names won't end with a number at all
    match = re.search(r'\d{4}', hostname)
    if match:
        first = match.group()[0]
        expected = DATACENTER_NUMBERING_PREFIX[datacenter]
        if first != expected:
            raise RuntimeError(f'Hostname expected to match {expected}###, got {match.group()} instead')

    if network in ('public', 'sandbox'):
        return f'{hostname}.wikimedia.org'

    return f'{hostname}.{datacenter}.wmnet'
