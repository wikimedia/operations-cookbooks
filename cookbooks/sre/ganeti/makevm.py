"""Create a new Virtual Machine in Ganeti

Examples:
    Create a Ganeti VM vmname.codfw.wmnet in the codfw Ganeti cluster on row B with 1 vCPUs, 3GB of RAM, 100GB of disk
    in the private network:

        makevm --vcpus 1 --memory 3 --disk 100 codfw_B vmname.codfw.wmnet

"""

import argparse
import logging

from spicerack.dns import Dns, DnsNotFound
from spicerack.ganeti import CLUSTERS_AND_ROWS, INSTANCE_LINKS
from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = 'Create a new virtual machine in Ganeti.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def _get_locations():
    """Generate short location names with datacenter and row for all Ganeti clusters."""
    locations = {}
    for cluster, rows in CLUSTERS_AND_ROWS.items():
        dc = cluster.split('.')[2]
        if len(rows) == 1:
            locations[dc] = (cluster, rows[0])
        else:
            for row in rows:
                locations['{dc}_{row}'.format(dc=dc, row=row)] = (cluster, row)

    return locations


def argument_parser():
    """Parse command-line arguments for this module per spicerack API."""
    def positive_int(param):
        """Type validator for argparse that accepts only positive integers."""
        value = int(param)
        if value <= 0:
            raise argparse.ArgumentTypeError('{param} is not a positive integer'.format(param=param))

        return value

    def valid_fqdn(param):
        """Type validator for argparse that verify the existence of the DNS records for the VM."""
        resolver = Dns()
        try:
            ips = resolver.resolve_ipv4(param)
            for ip in ips:
                resolver.resolve_ptr(ip)
        except DnsNotFound as e:
            raise argparse.ArgumentTypeError('missing DNS records for FQDN {fqdn}: {e}'.format(fqdn=param, e=e))

        return param

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)

    parser.add_argument(
        '--vcpus', type=positive_int, default=1, help='The number of virtual CPUs to assign to the VM.')
    parser.add_argument(
        '--memory', type=positive_int, default=1, help='The amount of RAM to allocate to the VM in GB.')
    parser.add_argument(
        '--disk', type=positive_int, default=10, help='The amount of disk to allocate to the VM in GB.')
    parser.add_argument('--network', choices=INSTANCE_LINKS, default='private',
                        help='Specify the type of network to assign to the VM.')
    parser.add_argument('location', choices=_get_locations().keys(),
                        help='The datacenter and row (only for multi-row clusters) where to create the VM')
    parser.add_argument('fqdn', type=valid_fqdn, help='The FQDN for the VM. The DNS records must exist.')

    return parser


def run(args, spicerack):
    """Create a new Ganeti VM as specified."""
    ensure_shell_is_durable()
    args.cluster, args.row = _get_locations()[args.location]  # Inject cluster and row in the args object.
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
