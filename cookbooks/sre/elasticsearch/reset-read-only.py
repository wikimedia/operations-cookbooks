"""Reset readonly status on all indices."""
import argparse
import logging

from cookbooks.sre.elasticsearch import CLUSTERGROUPS

__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(prog=__name__, description=__title__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup, args.write_queue_datacenters)

    elasticsearch_clusters.reset_indices_to_read_write()
