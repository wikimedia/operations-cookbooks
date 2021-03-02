"""DNS Discovery Operations"""
import argparse
import logging
import time

from spicerack.confctl import ConfctlError
from spicerack.constants import CORE_DATACENTERS

from cookbooks import ArgparseFormatter
from cookbooks.sre.discovery import check_record_for_dc, wipe_recursor_cache, update_ttl

__title__ = __doc__
logger = logging.getLogger(__name__)

# Fixme: Move to spicerack.constants
# DNS_TTL_LONG = 3600
DNS_TTL_MEDIUM = 300
DNS_TTL_SHORT = 10


def argument_parser():
    """Parse the command line arguments for all the sre.discovery cookbooks."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    actions = parser.add_subparsers(dest='action', help='The action to perform')
    actions.add_parser('check')
    pool = actions.add_parser('pool')
    depool = actions.add_parser('depool')

    for a in (pool, depool):
        a.add_argument('datacenter', choices=CORE_DATACENTERS, help='Name of the datacenter. One of: %(choices)s.')
        a.add_argument('--wipe-cache', action='store_true', help='Wipe the cache on DNS recursors.')

    parser.add_argument('services', nargs='+', help='The services to operate on')

    return parser


def check(args, spicerack):
    """Check the current state of the service in conftool and on authoritative DNS servers."""
    dnsdisc = spicerack.discovery(*args.services)
    try:
        active_dcs = dnsdisc.active_datacenters
    except ConfctlError as e:
        logger.error('dnsdisc %s: %s', args.services, e)
        return 1

    print('Expected routes:')
    for svc in args.services:
        svc_active_dcs = active_dcs.get(svc, [])
        route = ','.join(sorted(svc_active_dcs))
        print('{service}: {route}'.format(service=svc, route=route))
        for dc in CORE_DATACENTERS:
            dc_to = dc
            if dc not in svc_active_dcs:
                # If DC is not active, the discovery record should point to the other DC
                # WARNING: This assumes we only have wo datacenters.
                dc_to = svc_active_dcs[0]

            # Not all discovery records have a <svc>.<dc>.wmnet record.
            # For instance, appservers-{rw,ro} will need to resolve appserver.svc.<dc>.wmnet.
            # WARNING: This is a hard coded assumption that may not cover all cases correctly.
            svc_to = svc
            for postfix in ('-rw', '-ro', '-async', '-php'):
                if svc_to.endswith(postfix):
                    svc_to = svc_to[:-len(postfix)]

            expected_name_fmt = '{service}.svc.{dc_to}.wmnet'
            if svc_to != svc:
                logger.info('Stripped prefix from expected target service name: %s -> %s',
                            expected_name_fmt.format(service=svc, dc_to=dc_to),
                            expected_name_fmt.format(service=svc_to, dc_to=dc_to))

            # Check if authdns reflects the conftool/etcd setting
            check_record_for_dc(spicerack.dry_run, dnsdisc, dc, svc,
                                expected_name_fmt.format(service=svc_to, dc_to=dc_to))
    return 0


def pool_or_depool(args, spicerack, depool: bool):
    """Pool/Depool services from given datacenters."""
    pool = not depool  # Just easier to read later on
    did_change = False

    dnsdisc = spicerack.discovery(*args.services)
    try:
        active_dcs = dnsdisc.active_datacenters
    except ConfctlError as e:
        logger.error('dnsdisc %s: %s', args.services, e)
        return 1

    if pool and not all(dc in active_dcs for dc in args.datacenter):
        old_ttl = update_ttl(dnsdisc, DNS_TTL_MEDIUM)
        dnsdisc.pool(args.datacenter)
        did_change = True

    if depool and any(dc in active_dcs for dc in args.datacenter):
        old_ttl = update_ttl(dnsdisc, DNS_TTL_SHORT)
        dnsdisc.depool(args.datacenter)
        did_change = True

    # Exit early if no changes where necessary
    if not did_change:
        return 0

    # The actual work is done now. DNS should be propagated in old_ttl seconds from now at the latest.
    records_propagated_at = time.time() + old_ttl

    if args.wipe_cache:
        wipe_recursor_cache(args, spicerack.remote())

    sleep_time = records_propagated_at - time.time()
    if sleep_time > 0:
        logging.info('Waiting %.2f seconds for DNS changes to propagate', sleep_time)
        if not spicerack.dry_run:
            time.sleep(sleep_time)

    # This just checks auth servers
    # Fixme: Check the availability of the records on the resolvers as well?
    return check(args, spicerack)


def run(args, spicerack):
    """Required by Spicerack API."""
    if args.action == 'check':
        return check(args, spicerack)
    if args.action == 'pool':
        return pool_or_depool(args, spicerack, depool=False)
    if args.action == 'depool':
        return pool_or_depool(args, spicerack, depool=True)

    return 0
