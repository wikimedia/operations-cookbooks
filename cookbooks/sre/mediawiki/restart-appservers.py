"""Safely restart one or multiple systemd services on application servers.

This cookbook will depool and repool appservers from LVS pools nginx and apache2
to restart services safely.

Examples:
    Restart mcrouter and nutcracker on the appservers in codfw and eqiad:
        restart_appservers -c appserver -d eqiad codfw -- mcrouter nutcracker
    Restart php7.4-fpm on the api appservers in eqiad (no more than 10% at a time):
        restart_appservers -p 10% -c api_appserver -d eqiad -- php7.4-fpm

"""

import argparse
import logging
import math

from wmflib.constants import CORE_DATACENTERS


# TODO: get this from a config file maybe?
CLUSTERS = {
    'jobrunner': 'jobrunner'}

# LVS pools that are affected by the service we're restarting.
POOLS = ['apache2', 'nginx']

logger = logging.getLogger(__name__)

__title__ = 'Restart services on various appserver clusters'


def check_percentage(arg):
    """Type checker for a percentage between 0 and 100."""
    try:
        int_arg = int(arg)
    except ValueError as e:
        raise argparse.ArgumentTypeError("Percentage must be an integer.") from e
    if int_arg < 1 or int_arg > 100:
        raise argparse.ArgumentTypeError("Percentage must be between 1 and 100")
    return int_arg


def argument_parser():
    """CLI parsing, as required by the Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--datacenters', '-d',
                        help='Datacenters where to restart the service',
                        choices=CORE_DATACENTERS, default=CORE_DATACENTERS, nargs='+')
    cluster_names = CLUSTERS.keys()
    parser.add_argument('--clusters', '-c', help='Clusters to restart',
                        choices=cluster_names, default=cluster_names, nargs="+")
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
                        nargs='+')
    return parser


def run(args, spicerack):
    """Required by the Spicerack API."""
    # Guard against useless conftool messages
    logging.getLogger("conftool").setLevel(logging.WARNING)
    confctl = spicerack.confctl('node')
    remote = spicerack.remote()
    # TODO: allow running these in parallel? Does spicerack even allow it?
    for cluster_name in args.clusters:
        for dc in args.datacenters:
            logger.info('Now acting on: %s/%s', cluster_name, dc)
            lbremote = remote.query_confctl(confctl, dc=dc, cluster=cluster_name)
            # 15% of a cluster
            perc = 0.01 * args.percentage
            batch_size = math.ceil(len(lbremote) * perc)
            logger.info('Will act on %s servers at a time', batch_size)
            callback = lbremote.restart_services
            if args.reload:
                callback = lbremote.reload_services

            result = callback(
                args.services,
                POOLS,  # on each server, we depool all the pools we declared
                batch_size=batch_size,
                batch_sleep=args.batch_sleep
            )
            for resultset in result:
                logger.info("Nodes %s: %s", resultset[0], str(resultset[1]))
