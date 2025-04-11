"""Cassandra cookbook to perform `nodetool repair`

Usage example:
    cookbook sre.cassandra.repair --query maps1003.eqiad.wmnet

"""
import argparse
import logging


logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for sre.cassandra.repair cookbooks."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='Cumin query to match the host(s) to act upon.')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    remote_hosts = spicerack.remote().query(args.query)
    remote_hosts.run_sync('nodetool repair', batch_size=1)
