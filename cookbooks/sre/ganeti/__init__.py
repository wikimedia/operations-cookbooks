"""Ganeti Cookbooks"""
import argparse


__title__ = __doc__


def add_location_args(parser: argparse.ArgumentParser) -> None:
    """Add to the given parser the Ganeti-related location arguments."""
    parser.add_argument(
        '--cluster',
        help=('The Ganeti cluster short name, as reported in Netbox as a Cluster Group: '
              'https://netbox.wikimedia.org/virtualization/cluster-groups/'))
    parser.add_argument(
        '--group',
        help=('The Ganeti group name, as reported in Netbox as a Cluster for the given group: '
              'https://netbox.wikimedia.org/virtualization/clusters/')
    )
