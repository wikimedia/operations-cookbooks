"""Safely restart one or multiple services on application servers

Examples:
    Restart mcrouter and nutcracker on the appservers in codfw and eqiad:
        restart_appservers -c appserver -d eqiad codfw -- mcrouter nutcracker

"""

import argparse
import logging
import math

from spicerack.constants import CORE_DATACENTERS

# TODO: get this from a config file maybe?
CLUSTERS = {
    'appserver': 'mw',
    'appserver_api': 'mw-api',
    'jobrunner': 'jobrunner'}

# LVS pools that are affected by the service we're restarting.
POOLS = ['apache2', 'nginx']

logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def get_title(args):
    """Title of the job, depending on the specified args."""
    to_restart = ', '.join(map(lambda x: x.capitalize()), args.services)
    what = 'restart'
    if args.reload:
        what = 'reload'
    return 'Rolling {w} of {r} in {d}, clusters: {c}'.format(
        w=what, r=to_restart, d=', '.join(args.datacenters),
        c=', '.join(args.clusters)
    )


def check_percentage(arg):
    """Type checker for a percentage between 0 and 100."""
    try:
        int_arg = int(arg)
    except ValueError:
        raise argparse.ArgumentTypeError("Percentage must be an integer.")
    if int_arg < 1 or int_arg > 100:
        raise argparse.ArgumentTypeError("Percentage must be between 1 and 100")
    return int_arg


def argument_parser():
    """CLI parsing, as required by the Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--datacenters', '-d',
                        help='Datacenters where to restart the service',
                        choice=CORE_DATACENTERS, default=CORE_DATACENTERS, nargs='+')
    cluster_names = CLUSTERS.keys()
    parser.add_argument('--clusters', '-c', help='Clusters to restart',
                        choice=cluster_names, default=cluster_names, nargs="+")
    parser.add_argument('--reload', '-r',
                        help='Reload the service instead of restarting it',
                        action='store_true')
    parser.add_argument('--percentage', '-p',
                        help='Percentage of the cluster to act upon at the same time',
                        type=check_percentage, default=15)
    parser.add_argument('--batch_sleep', '-s',
                        help='sleep between batches in seconds. Defaults to 2',
                        default=2.0,
                        type=float)
    parser.add_argument('services', help='Services to restart', metavar='SERVICE',
                        nargs='+',
                        required=True)
    return parser


def run(args, spicerack):
    """Required by the Spicerack API."""
    confctl = spicerack.confctl('node')
    remote = spicerack.remote()
    # TODO: allow running these in parallel? Does spicerack even allow it?
    for cluster_name in args.clusters:
        for dc in args.datacenters:
            logger.info('Now acting on: {cl}/{dc}'.format(cl=cluster_name, dc=dc))
            lbremote = remote.query_confctl(confctl, dc=dc, cluster=cluster_name)
            # 15% of a cluster
            perc = 0.01 * args.percentage
            batch_size = math.ceil(len(lbremote) * perc)

            callback = lbremote.restart_services
            if args.reload:
                callback = lbremote.reload_services

            callback(
                args.services,
                POOLS,  # on each server, we depool all the the pools we declared
                batch_size=batch_size,
                batch_sleep=args.batch_sleep
            )
