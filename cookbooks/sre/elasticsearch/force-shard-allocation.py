"""Force allocation of all shards"""
import argparse
import logging

from spicerack.constants import CORE_DATACENTERS
from cookbooks.sre.elasticsearch import CLUSTERGROUPS

__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(prog=__name__, description=__title__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    parser.add_argument('--write-queue-datacenters', choices=CORE_DATACENTERS, default=CORE_DATACENTERS, nargs='+',
                        help='Manually specify a list of specific datacenters to check the '
                             'cirrus write queue rather than checking all core datacenters (default)')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup, args.write_queue_datacenters)

    elasticsearch_clusters.force_allocation_of_all_unassigned_shards()
