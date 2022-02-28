"""Create a new Virtual Machine in Ganeti"""

import argparse
import logging
import re

from wmflib.constants import DATACENTER_NUMBERING_PREFIX
from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.constants import CORE_DATACENTERS
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.ganeti import INSTANCE_LINKS

from cookbooks.sre.ganeti import get_locations

logger = logging.getLogger(__name__)
PRIMARY_INTERFACE_NAME = '##PRIMARY##'
PER_RACK_VLAN_DATACENTERS = ('drmrs',)


class GanetiMakeVM(CookbookBase):
    """Create a new Virtual Machine in Ganeti

    * Pre-allocate the primary IPs and set their DNS name
    * Update the DNS records
    * Create the VM on Ganeti
    * Force a sync of Ganeti VMs to Netbox in the same DC
    * Update Netbox attaching the pre-allocated IPs to the host's primary interface

    Examples:
        Create a Ganeti VM vmname.codfw.wmnet in the codfw Ganeti cluster
        on row B with 1 vCPUs, 3GB of RAM, 100GB of disk in the private network:

            makevm --vcpus 1 --memory 3 --disk 100 codfw_B vmhostname

    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        def positive_int(param):
            """Type validator for argparse that accepts only positive integers."""
            value = int(param)
            if value <= 0:
                raise argparse.ArgumentTypeError('{param} is not a positive integer'.format(param=param))

            return value

        def validate_hostname(param):
            """Helper to instruct people to pass in a hostname instead of fqdn"""
            if '.' in param:
                raise argparse.ArgumentTypeError('This cookbook now takes a hostname, not a fqdn')
            return param

        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)

        parser.add_argument('--skip-v6', action='store_true', help='To skip the generation of the IPv6 DNS record.')
        parser.add_argument(
            '--vcpus', type=positive_int, default=1, help='The number of virtual CPUs to assign to the VM.')
        parser.add_argument(
            '--memory', type=positive_int, default=1, help='The amount of RAM to allocate to the VM in GB.')
        parser.add_argument(
            '--disk', type=positive_int, default=10, help='The amount of disk to allocate to the VM in GB.')
        parser.add_argument('--network', choices=INSTANCE_LINKS, default='private',
                            help='Specify the type of network to assign to the VM.')
        parser.add_argument('location', choices=sorted(get_locations().keys()),
                            help='The datacenter and row (only for multi-row clusters) where to create the VM.')
        parser.add_argument('hostname', type=validate_hostname, help='The hostname for the VM (not the FQDN).')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiMakeVMRunner(args, self.spicerack)


class GanetiMakeVMRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """Create a new Virtual Machine in Ganeti runner"""

    def __init__(self, args, spicerack):
        """Create a new Virtual Machine in Ganeti."""
        self.cluster, self.row, self.datacenter = get_locations()[args.location]
        self.hostname = args.hostname
        self.vcpus = args.vcpus
        self.memory = args.memory
        self.network = args.network
        self.disk = args.disk
        self.skip_v6 = args.skip_v6
        self.spicerack = spicerack
        self.netbox = self.spicerack.netbox(read_write=True)
        self.fqdn = make_fqdn(self.hostname, self.network, self.datacenter)
        self.allocated = []  # Store allocated IPs to rollback them on failure
        self.dns_propagated = False  # Whether to run the DNS cookbook on rollback
        self.need_netbox_sync = False  # Whether to sync the VM to Netbox on rollback

        print('Ready to create Ganeti VM {a.fqdn} in the {a.cluster} cluster on row {a.row} with {a.vcpus} vCPUs, '
              '{a.memory}GB of RAM, {a.disk}GB of disk in the {a.network} network.'.format(a=self))
        ask_confirmation('Is this correct?')

        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for new host {}'.format(self.fqdn)

    def rollback(self):
        """Rollback IP and DNS assignments on failure."""
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
        confirm_on_failure(self.spicerack.run_cookbook, 'sre.dns.netbox', [f'{prefix} records for VM {self.fqdn}'])
        self.dns_propagated = True

    def _ganeti_netbox_sync(self):
        """Perform a sync from Ganeti to Netbox in the affected DC."""
        logger.info('Syncing VMs in DC %s to Netbox', self.datacenter)
        cluster_id = ''
        if self.datacenter in PER_RACK_VLAN_DATACENTERS:
            cluster_id = self.cluster.split('.')[0][-2:]
        self.spicerack.netbox_master_host.run_sync(
            'systemctl start netbox_ganeti_{dc}{cluster_id}_sync.service'
            .format(dc=self.datacenter, cluster_id=cluster_id))
        self.need_netbox_sync = False

    def run(self):  # pylint: disable=too-many-locals
        """Create a new Ganeti VM as specified."""
        # Pre-allocate IPs
        if self.datacenter in CORE_DATACENTERS or self.datacenter in PER_RACK_VLAN_DATACENTERS:
            vlan_name = '{a.network}1-{row}-{a.datacenter}'.format(a=self, row=self.row.lower())
        else:
            vlan_name = '{a.network}1-{a.datacenter}'.format(a=self)

        vlan = self.netbox.api.ipam.vlans.get(name=vlan_name, status='active')
        if not vlan:
            raise RuntimeError('Failed to find VLAN with name {}'.format(vlan_name))

        prefix_v4 = self.netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=4)
        prefix_v6 = self.netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=6)
        ip_v4_data = prefix_v4.available_ips.create({})
        self.allocated.append(ip_v4_data['address'])
        logger.info('Allocated IPv4 %s', ip_v4_data['address'])
        ip_v4 = self.netbox.api.ipam.ip_addresses.get(address=ip_v4_data['address'])
        ip_v4.dns_name = self.fqdn
        if not ip_v4.save():
            raise RuntimeError(
                'Failed to save DNS name for IP {} on Netbox'.format(ip_v4))

        logger.info('Set DNS name of IP %s to %s', ip_v4, self.fqdn)

        # Generate the IPv6 address embedding the IPv4 address, for example from an IPv4 address 10.0.0.1 and an
        # IPv6 prefix 2001:db8:3c4d:15::/64 the mapped IPv6 address 2001:db8:3c4d:15:10:0:0:1/64 is generated.
        prefix_v6_base, prefix_v6_mask = str(prefix_v6).split("/")
        mapped_v4 = str(ip_v4).split('/', maxsplit=1)[0].replace(".", ":")
        ipv6_address = '{prefix}:{mapped}/{mask}'.format(
            prefix=prefix_v6_base.rstrip(':'), mapped=mapped_v4, mask=prefix_v6_mask)
        if self.skip_v6:
            dns_name_v6 = ''
        else:
            dns_name_v6 = self.fqdn
        ip_v6 = self.netbox.api.ipam.ip_addresses.create(address=ipv6_address, status='active', dns_name=dns_name_v6)
        self.allocated.append(ip_v6.address)
        logger.info('Allocated IPv6 %s with DNS name %s', ip_v6, dns_name_v6)

        self._propagate_dns('Add')

        # Create the VM
        ganeti = self.spicerack.ganeti()
        instance = ganeti.instance(self.fqdn, cluster=self.cluster)

        logger.info('The Ganeti\'s command output will be printed at the end.')

        self.need_netbox_sync = True
        instance.add(
            row=self.row, vcpus=self.vcpus, memory=self.memory,
            disk=self.disk, link=self.network)

        if self.spicerack.dry_run:
            logger.info('Skipping MAC address retrieval in DRY-RUN mode.')
        else:
            mac = ganeti.rapi(self.cluster).fetch_instance_mac(self.fqdn)
            logger.info('MAC address for %s is: %s', self.fqdn, mac)

        self._ganeti_netbox_sync()

        # Get the synced VM
        @retry(tries=20, backoff_mode='linear', exceptions=(RuntimeError,))
        def get_vm(netbox):
            vm = netbox.api.virtualization.virtual_machines.get(name=self.hostname)
            if not vm:
                raise RuntimeError('VM {host} not yet found on Netbox'.format(host=self.hostname))

            return vm

        # Update Netbox
        vm = get_vm(self.netbox)
        iface = self.netbox.api.virtualization.interfaces.create(
            virtual_machine=vm.id, name=PRIMARY_INTERFACE_NAME, type='virtual')
        logger.info('Created interface %s on VM %s', PRIMARY_INTERFACE_NAME, vm)

        ip_v4.assigned_object_id = iface.id
        ip_v4.assigned_object_type = 'virtualization.vminterface'
        if not ip_v4.save():
            raise RuntimeError(
                'Failed to attach IPv4 {} to interface {}'.format(ip_v4, iface))

        ip_v6.assigned_object_id = iface.id
        ip_v6.assigned_object_type = 'virtualization.vminterface'
        if not ip_v6.save():
            raise RuntimeError(
                'Failed to attach IPv6 {} to interface {}'.format(ip_v6, iface))

        vm.primary_ip4 = ip_v4
        vm.primary_ip6 = ip_v6
        if not vm.save():
            raise RuntimeError('Failed to set primary IPv4/6 to VM {}'.format(vm))

        logger.info(
            'Attached IPv4 %s and IPv6 %s to VM %s and marked as primary IPs',
            ip_v4, ip_v6, vm)


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

    if network == 'public':
        return f'{hostname}.wikimedia.org'

    return f'{hostname}.{datacenter}.wmnet'
