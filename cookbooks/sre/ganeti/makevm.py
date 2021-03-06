"""Create a new Virtual Machine in Ganeti"""

import argparse
import logging

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.constants import CORE_DATACENTERS
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.ganeti import INSTANCE_LINKS

from cookbooks import ArgparseFormatter
from cookbooks.sre.dns.netbox import argument_parser as dns_netbox_argparse, run as dns_netbox_run
from cookbooks.sre.ganeti import get_locations

logger = logging.getLogger(__name__)
PRIMARY_INTERFACE_NAME = '##PRIMARY##'


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

            makevm --vcpus 1 --memory 3 --disk 100 codfw_B vmname.codfw.wmnet

    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        def positive_int(param):
            """Type validator for argparse that accepts only positive integers."""
            value = int(param)
            if value <= 0:
                raise argparse.ArgumentTypeError('{param} is not a positive integer'.format(param=param))

            return value

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
        parser.add_argument('fqdn', help='The FQDN for the VM.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiMakeVMRunner(args, self.spicerack)


class GanetiMakeVMRunner(CookbookRunnerBase):
    """Create a new Virtual Machine in Ganeti runner"""

    def __init__(self, args, spicerack):
        """Create a new Virtual Machine in Ganeti."""
        self.cluster, self.row, self.datacenter = get_locations()[args.location]
        self.hostname = args.fqdn.split('.')[0]
        self.fqdn = args.fqdn
        self.vcpus = args.vcpus
        self.memory = args.memory
        self.network = args.network
        self.disk = args.disk
        self.skip_v6 = args.skip_v6
        self.spicerack = spicerack

        print('Ready to create Ganeti VM {a.fqdn} in the {a.cluster} cluster on row {a.row} with {a.vcpus} vCPUs, '
              '{a.memory}GB of RAM, {a.disk}GB of disk in the {a.network} network.'.format(a=self))
        ask_confirmation('Is this correct?')

        ensure_shell_is_durable()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for new host {}'.format(self.fqdn)

    def run(self):  # pylint: disable=too-many-locals,too-many-statements
        """Create a new Ganeti VM as specified."""
        netbox = self.spicerack.netbox(read_write=True)
        # Pre-allocate IPs
        if self.datacenter in CORE_DATACENTERS:
            vlan_name = '{a.network}1-{row}-{a.datacenter}'.format(a=self, row=self.row.lower())
        else:
            vlan_name = '{a.network}1-{a.datacenter}'.format(a=self)

        vlan = netbox.api.ipam.vlans.get(name=vlan_name, status='active')
        if not vlan:
            raise RuntimeError('Failed to find VLAN with name {}'.format(vlan_name))

        prefix_v4 = netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=4)
        prefix_v6 = netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=6)
        ip_v4_data = prefix_v4.available_ips.create({})
        logger.info('Allocated IPv4 %s', ip_v4_data['address'])
        ip_v4 = netbox.api.ipam.ip_addresses.get(address=ip_v4_data['address'])
        ip_v4.dns_name = self.fqdn
        if not ip_v4.save():
            raise RuntimeError(
                'Failed to save DNS name for IP {} on Netbox'.format(ip_v4))

        logger.info('Set DNS name of IP %s to %s', ip_v4, self.fqdn)

        # Generate the IPv6 address embedding the IPv4 address, for example from an IPv4 address 10.0.0.1 and an
        # IPv6 prefix 2001:db8:3c4d:15::/64 the mapped IPv6 address 2001:db8:3c4d:15:10:0:0:1/64 is generated.
        prefix_v6_base, prefix_v6_mask = str(prefix_v6).split("/")
        mapped_v4 = str(ip_v4).split('/')[0].replace(".", ":")
        ipv6_address = '{prefix}:{mapped}/{mask}'.format(
            prefix=prefix_v6_base.rstrip(':'), mapped=mapped_v4, mask=prefix_v6_mask)
        if self.skip_v6:
            dns_name_v6 = ''
        else:
            dns_name_v6 = self.fqdn
        ip_v6 = netbox.api.ipam.ip_addresses.create(address=ipv6_address, status='active', dns_name=dns_name_v6)
        logger.info('Allocated IPv6 %s with DNS name %s', ip_v6, dns_name_v6)

        # Run the sre.dns.netbox cookbook to generate the DNS records
        dns_netbox_args = dns_netbox_argparse().parse_args(
            ['Created records for VM {vm}'.format(vm=self.fqdn)])
        dns_netbox_run(dns_netbox_args, self.spicerack)

        # Create the VM
        ganeti = self.spicerack.ganeti()
        instance = ganeti.instance(self.fqdn, cluster=self.cluster)

        logger.info('The Ganeti\'s command output will be printed at the end.')

        instance.add(
            row=self.row, vcpus=self.vcpus, memory=self.memory,
            disk=self.disk, link=self.network)

        if self.spicerack.dry_run:
            logger.info('Skipping MAC address retrieval in DRY-RUN mode.')
        else:
            mac = ganeti.rapi(self.cluster).fetch_instance_mac(self.fqdn)
            logger.info('MAC address for %s is: %s', self.fqdn, mac)

        # Force a run of the ganeti sync
        logger.info('Syncing VMs in DC %s to Netbox', self.datacenter)
        self.spicerack.netbox_master_host.run_sync(
            'systemctl start netbox_ganeti_{dc}_sync.service'
            .format(dc=self.datacenter))

        # Get the synced VM
        @retry(tries=20, backoff_mode='linear', exceptions=(RuntimeError,))
        def get_vm(netbox):
            vm = netbox.api.virtualization.virtual_machines.get(name=self.hostname)
            if not vm:
                raise RuntimeError

            return vm

        # Update Netbox
        vm = get_vm(netbox)
        iface = netbox.api.virtualization.interfaces.create(
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

        # TODO: run the Netbox import script for the VM after the first Puppet run to import all interfaces
