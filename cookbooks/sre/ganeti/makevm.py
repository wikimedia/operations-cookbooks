"""Create a new Virtual Machine in Ganeti

* Pre-allocate the primary IPs and set their DNS name
* Update the DNS records
* Create the VM on Ganeti
* Force a sync of Ganeti VMs to Netbox in the same DC
* Update Netbox attaching the pre-allocated IPs to the host's primary interface

Examples:
    Create a Ganeti VM vmname.codfw.wmnet in the codfw Ganeti cluster on row B with 1 vCPUs, 3GB of RAM, 100GB of disk
    in the private network:

        makevm --vcpus 1 --memory 3 --disk 100 codfw_B vmname.codfw.wmnet

"""

import argparse
import ipaddress
import logging

from spicerack.constants import CORE_DATACENTERS
from spicerack.decorators import retry
from spicerack.ganeti import CLUSTERS_AND_ROWS, INSTANCE_LINKS
from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter
from cookbooks.sre.dns.netbox import argument_parser as dns_netbox_argparse, run as dns_netbox_run


__title__ = 'Create a new virtual machine in Ganeti.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
MIGRATED_PRIMARY_SITES = ('ulsfo',)
PRIMARY_INTERFACE_NAME = '##PRIMARY##'


def _get_locations():
    """Generate short location names with datacenter and row for all Ganeti clusters."""
    locations = {}
    for cluster, rows in CLUSTERS_AND_ROWS.items():
        dc = cluster.split('.')[2]
        if len(rows) == 1:
            locations[dc] = (cluster, rows[0], dc)
        else:
            for row in rows:
                locations['{dc}_{row}'.format(dc=dc, row=row)] = (cluster, row, dc)

    return locations


def argument_parser():
    """Parse command-line arguments for this module per spicerack API."""
    def positive_int(param):
        """Type validator for argparse that accepts only positive integers."""
        value = int(param)
        if value <= 0:
            raise argparse.ArgumentTypeError('{param} is not a positive integer'.format(param=param))

        return value

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)

    parser.add_argument('--skip-v6', action='store_true', help='To skip the generation of the IPv6 DNS record.')
    parser.add_argument(
        '--vcpus', type=positive_int, default=1, help='The number of virtual CPUs to assign to the VM.')
    parser.add_argument(
        '--memory', type=positive_int, default=1, help='The amount of RAM to allocate to the VM in GB.')
    parser.add_argument(
        '--disk', type=positive_int, default=10, help='The amount of disk to allocate to the VM in GB.')
    parser.add_argument('--network', choices=INSTANCE_LINKS, default='private',
                        help='Specify the type of network to assign to the VM.')
    parser.add_argument('location', choices=sorted(_get_locations().keys()),
                        help='The datacenter and row (only for multi-row clusters) where to create the VM.')
    parser.add_argument('fqdn', help='The FQDN for the VM.')

    return parser


def run(args, spicerack):
    """Create a new Ganeti VM as specified."""
    ensure_shell_is_durable()
    args.cluster, args.row, args.dc = _get_locations()[args.location]  # Inject cluster, row and dc in the args object.
    hostname = args.fqdn.split('.')[0]

    # Pre-allocate IPs
    netbox = spicerack.netbox(read_write=True)
    if args.dc in CORE_DATACENTERS:
        vlan_name = '{a.network}1-{row}-{a.dc}'.format(a=args, row=args.row.lower())
    else:
        vlan_name = '{a.network}1-{a.dc}'.format(a=args)

    vlan = netbox.api.ipam.vlans.get(name=vlan_name, status='active')
    if not vlan:
        logger.error('Failed to find VLAN with name %s', vlan_name)
        return 1

    prefix_v4 = netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=4)
    prefix_v6 = netbox.api.ipam.prefixes.get(vlan_id=vlan.id, family=6)
    ip_v4_data = prefix_v4.available_ips.create({})
    logger.info('Allocated IPv4 %s', ip_v4_data['address'])
    ip_v4 = netbox.api.ipam.ip_addresses.get(address=ip_v4_data['address'])
    ip_v4.dns_name = args.fqdn
    if not ip_v4.save():
        logger.error('Failed to save DNS name for IP %s on Netbox', ip_v4)
        return 1

    logger.info('Set DNS name of IP %s to %s', ip_v4, args.fqdn)

    # Generate the IPv6 address embedding the IPv4 address, for example from an IPv4 address 10.0.0.1 and an
    # IPv6 prefix 2001:db8:3c4d:15::/64 the mapped IPv6 address 2001:db8:3c4d:15:10:0:0:1/64 is generated.
    prefix_v6_base, prefix_v6_mask = str(prefix_v6).split("/")
    mapped_v4 = str(ip_v4).split('/')[0].replace(".", ":")
    ipv6_address = '{prefix}:{mapped}/{mask}'.format(
        prefix=prefix_v6_base.rstrip(':'), mapped=mapped_v4, mask=prefix_v6_mask)
    if args.skip_v6:
        dns_name_v6 = ''
    else:
        dns_name_v6 = args.fqdn
    ip_v6 = netbox.api.ipam.ip_addresses.create(address=ipv6_address, status='active', dns_name=dns_name_v6)
    logger.info('Allocated IPv6 %s with DNS name %s', ip_v6, dns_name_v6)

    # Run the sre.dns.netbox cookbook to generate the DNS records
    dns_netbox_args = dns_netbox_argparse().parse_args(['Created records for VM {vm}'.format(vm=args.fqdn)])
    dns_netbox_run(dns_netbox_args, spicerack)

    if args.dc not in MIGRATED_PRIMARY_SITES:
        ip_v4_address = ipaddress.ip_interface(ip_v4).ip
        ip_v6_address = ipaddress.ip_interface(ip_v6).ip
        if args.skip_v6:
            ip_v6_message = ''
        else:
            ip_v6_message = '\n    IPv6: {ip}\n    PTRv6: {ptr}\n    DNSv6: {dns}'.format(
                ip=ip_v6_address, ptr=ip_v6_address.reverse_pointer, dns=dns_name_v6)

        logger.warning('DC %s has not yet been migrated for primary records. Manual commit in the operations/dns '
                       'repository is required. See '
                       'https://wikitech.wikimedia.org/wiki/Server_Lifecycle/DNS_Transition'
                       '\n\nPlease make a patch to the DNS repository for the following data:'
                       '\n\n    IPv4:  %s\n    PTRv4: %s'
                       '\n    DNSv4: %s%s',
                       args.dc, ip_v4_address, ip_v4_address.reverse_pointer, args.fqdn, ip_v6_message)

        ask_confirmation('Proceed only when the DNS patch has been merged and deployed')

    # Create the VM
    ganeti = spicerack.ganeti()
    instance = ganeti.instance(args.fqdn, cluster=args.cluster)

    print('Ready to create Ganeti VM {a.fqdn} in the {a.cluster} cluster on row {a.row} with {a.vcpus} vCPUs, '
          '{a.memory}GB of RAM, {a.disk}GB of disk in the {a.network} network.'.format(a=args))
    ask_confirmation('Is this correct?')
    logger.info('The command output will be printed at the end.')

    instance.add(row=args.row, vcpus=args.vcpus, memory=args.memory, disk=args.disk, link=args.network)

    if spicerack.dry_run:
        logger.info('Skipping MAC address retrieval in DRY-RUN mode.')
    else:
        mac = ganeti.rapi(args.cluster).fetch_instance_mac(args.fqdn)
        logger.info('MAC address for %s is: %s', args.fqdn, mac)

    # Force a run of the ganeti sync
    logger.info('Syncing VMs in DC %s to Netbox', args.dc)
    spicerack.netbox_master_host.run_sync('systemctl start netbox_ganeti_{dc}_sync.service'.format(dc=args.dc))

    # Get the synced VM
    @retry(tries=20, backoff_mode='linear', exceptions=(RuntimeError,))
    def get_vm(netbox):
        vm = netbox.api.virtualization.virtual_machines.get(name=hostname)
        if not vm:
            raise RuntimeError

        return vm

    # Update Netbox
    vm = get_vm(netbox)
    iface = netbox.api.virtualization.interfaces.create(
        virtual_machine=vm.id, name=PRIMARY_INTERFACE_NAME, type='virtual')
    logger.info('Created interface %s on VM %s', PRIMARY_INTERFACE_NAME, vm)

    ip_v4.interface = iface
    if not ip_v4.save():
        logger.error('Failed to attach IPv4 %s to interface %s', ip_v4, iface)
        return 1

    ip_v6.interface = iface
    if not ip_v6.save():
        logger.error('Failed to attach IPv6 %s to interface %s', ip_v6, iface)
        return 1

    vm.primary_ip4 = ip_v4
    vm.primary_ip6 = ip_v6
    if not vm.save():
        logger.error('Failed to set primary IPv4/6 to VM %s', vm)
        return 1

    logger.info('Attached IPv4 %s and IPv6 %s to VM %s and marked as primary IPs', ip_v4, ip_v6, vm)

    # TODO: run the Netbox import script for the VM after the first Puppet run to import all interfaces
