"""Create a new Virtual Machine in Ganeti

Examples:
    Create a VM with 1 VCPU and 3 gigabytes of ram and 100 gigabytes of disk on codfw:

        makevm codfw_B mytestvm.codfw.wmnet --vcpus 1 --memory 3 --disk 100

"""

import argparse
import logging

from spicerack.ganeti import CLUSTERS_AND_ROWS
from spicerack.interactive import ask_confirmation

__title__ = 'Create a new virtual machine in Ganeti.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

CREATE_COMMAND = (
    'gnt-instance add -t drbd -I hail'
    ' --net 0:link={link}'
    ' --hypervisor-parameters=kvm:boot_order=network'
    ' -o bootstrap+default'
    ' --no-install'
    ' -g row_{row}'
    ' -B vcpus={vcpus},memory={memory}g'
    ' --disk 0:size={disk}g'
    ' {fqdn}'
)


def argument_parser():
    """Parse command-line arguments for this module per spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)

    # build option list from cluster definitions
    clusters_and_rows = []
    for cluster, rows in CLUSTERS_AND_ROWS.items():
        for row in rows:
            clusters_and_rows.append(cluster + '_' + row)

    parser.add_argument(
        'cluster_and_row',
        choices=clusters_and_rows,
        help='Ganeti cluster identifier %(choices)s',
    )
    parser.add_argument('fqdn', help='The FQDN for the VM.')
    parser.add_argument(
        '--vcpus', help='The number of virtual CPUs to assign to VM (default: %(default)s).', type=int, default=1
    )
    parser.add_argument(
        '--memory',
        help='The number of gigabytes of ram to allocate to the VM (default: %(default)s).',
        type=int,
        default=1,
    )
    parser.add_argument(
        '--disk', help='Number of gigabytes of disk to allocate to the VM (default: %(default)s).', type=int, default=10
    )
    parser.add_argument(
        '--link', help='Specify if a private, analytics or public IP address is required (default: %(default)s).',
        choices=['public', 'private', 'analytics'], default='private'
    )
    return parser


def run(args, spicerack):
    """Create a new Ganeti VM as specified."""
    # grab the cluster master from RAPI
    cluster, row = args.cluster_and_row.split('_')
    ganeti = spicerack.ganeti()
    cluster_fqdn = ganeti.rapi(cluster).master
    ganeti_host = spicerack.remote().query(cluster_fqdn)
    link = args.link

    # Create command line
    command = CREATE_COMMAND.format(
        link=link, row=row, vcpus=args.vcpus, memory=args.memory, disk=args.disk, fqdn=args.fqdn
    )

    logger.info(
        'Creating new VM named %s in %s with row=%s vcpu=%d memory=%d gigabytes disk=%d gigabytes',
        args.fqdn,
        cluster,
        row,
        args.vcpus,
        args.memory,
        args.disk,
    )

    ask_confirmation('Is this correct?')

    results = ganeti_host.run_sync(command)

    for _, output in results:
        logger.info(output.message().decode())

    # get MAC address of instance
    logger.info('instance %s created with MAC %s', args.fqdn, ganeti.rapi(cluster).fetch_instance_mac(args.fqdn))
