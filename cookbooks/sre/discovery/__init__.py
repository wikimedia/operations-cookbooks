"""DNS Discovery Operations"""
import argparse
import logging
import time

from spicerack.confctl import ConfctlError
from spicerack.constants import CORE_DATACENTERS
from cookbooks import ArgparseFormatter

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser_base(name, description, default_ttl):
    """Parse the command line arguments for all the sre.discovery cookbooks."""
    parser = argparse.ArgumentParser(prog=name, description=description,
                                     formatter_class=ArgparseFormatter)
    parser.add_argument('record', help='Name of the dnsdisc record/service')
    parser.add_argument('datacenter', choices=CORE_DATACENTERS, help='Name of the datacenter. One of: %(choices)s.')
    parser.add_argument('--ttl', type=int, default=default_ttl, help='The TTL to set the discovery record to')

    return parser


def run_base(args, spicerack, depool: bool):
    """Generic run method for pool/depool cookbooks.

    Arguments:
        args: argparse parsed arguments
        spicerack: spicerack instance
        depool (bool): True for depooling, False for pooling

    """
    pool = not depool  # Just easier to read later on
    did_change = False  # Will be set to True if we actually changed something
    records_propagated_at = 0.0  # Will be set to the time dnsdisc record TTL runs out

    dnsdisc = spicerack.discovery(args.record)
    try:
        active_dcs = dnsdisc.active_datacenters.get(args.record)
    except ConfctlError as e:
        logger.error('dnsdisc %s: %s', args.record, e)
        return 1

    # Get the old TTL
    # Fixme: Should get TTL from conftool instead of DNS but its not exposed currently
    #   https://phabricator.wikimedia.org/T259875
    old_ttl = max([r.ttl for r in dnsdisc.resolve()])
    if old_ttl == args.ttl:
        logger.info('TTL already set to %d, nothing to do', args.ttl)
    else:
        # Fixme: It's currently not possible to update the TTL for only one DC
        dnsdisc.update_ttl(args.ttl)
        did_change = True

    if depool:
        if args.datacenter not in active_dcs:
            logger.info('%s is not pooled in %s', args.record, args.datacenter)
        else:
            dnsdisc.depool(args.datacenter)
            did_change = True

    if pool:
        if args.datacenter in active_dcs:
            logger.info('%s is already pooled in %s', args.record, args.datacenter)
        else:
            dnsdisc.pool(args.datacenter)
            did_change = True

    # Exit early if no changes where necessary
    if not did_change:
        return 0

    # The actual work is done now. DNS should be propagated in old_ttl seconds from now at the latest.
    records_propagated_at = time.time() + old_ttl

    # Wipe the cache on resolvers to ensure they get updated quickly
    #   https://wikitech.wikimedia.org/wiki/DNS#How_to_Remove_a_record_from_the_DNS_resolver_caches
    recursor_hosts = spicerack.remote().query('A:dns-rec')
    wipe_cache_cmd = 'rec_control wipe-cache {record}.discovery.wmnet'.format(record=args.record)
    recursor_hosts.run_async(wipe_cache_cmd)

    # Fixme: We should check if resolvers have been updated
    sleep_time = records_propagated_at - time.time()
    if sleep_time > 0:
        logging.info('Waiting %.2f seconds for DNS changes to propagate', sleep_time)
        if not spicerack.dry_run:
            time.sleep(sleep_time)

    return 0
