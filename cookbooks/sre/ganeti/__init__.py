"""Ganeti Cookbooks"""
import argparse


__title__ = __doc__
__owner_team__ = 'Infrastructure Foundations'


def add_location_args(parser: argparse.ArgumentParser) -> None:
    """Add to the given parser the Ganeti-related location arguments."""
    parser.add_argument(
        '--cluster',
        required=True,
        help=('The Ganeti cluster short name, as reported in Netbox as a Cluster Group: '
              'https://netbox.wikimedia.org/virtualization/cluster-groups/'))
    parser.add_argument(
        '--group',
        help=('The Ganeti group name, as reported in Netbox as a Cluster for the given Cluster Group: '
              'https://netbox.wikimedia.org/virtualization/clusters/. It can be omitted in case the cluster has only '
              'one group.')
    )


def set_default_group(netbox, args) -> None:
    """Get the Ganeti group name in the cluster if there is just one group, raise RuntimeError if there are many."""
    if args.group:  # The group is already set
        return
    groups = netbox.api.virtualization.clusters.filter(group=args.cluster)
    if len(groups) == 0:
        raise RuntimeError(f'Ganeti cluster group {args.cluster} in Netbox has no cluster')
    if len(groups) != 1:
        raise RuntimeError(f'Ganeti cluster group {args.cluster} in Netbox has {len(groups)} clusters, specify one '
                           'via the --group argument.')

    args.group = next(groups).name
