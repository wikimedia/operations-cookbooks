"""Unfreeze writes forcefully in case they are stuck to frozen."""

import argparse
import logging

from cookbooks.sre.elasticsearch import CLUSTERGROUPS

logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(prog=__name__, description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    parser.add_argument('admin_reason', help='Administrative Reason')
    parser.add_argument('--task-id', help='task_id for the change')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup, args.write_queue_datacenters)
    reason = spicerack.admin_reason(args.admin_reason, task_id=args.task_id)

    # If frozen state is in an unstable state, freezing and thawing writes can
    # restore a normal situation. See incident report below for details.
    # https://wikitech.wikimedia.org/wiki/Incident_documentation/20190327-elasticsearch
    with elasticsearch_clusters.frozen_writes(reason):
        logger.info('Thawing writes again!')
