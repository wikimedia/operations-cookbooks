"""Switch MediaWiki active datacenter"""
import logging
import time

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, DNS_SHORT_TTL, MEDIAWIKI_SERVICES, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Switch MediaWiki active datacenter to %s', args.dc_to)

    dnsdisc_records = spicerack.discovery(*MEDIAWIKI_SERVICES)
    mediawiki = spicerack.mediawiki()

    # Pool DNS discovery records on the new dc.
    # This will NOT trigger confd to change the DNS admin state as it will cause a validation error
    dnsdisc_records.pool(args.dc_to)

    # Switch MediaWiki master datacenter
    start = time.time()
    mediawiki.set_master_datacenter(args.dc_to)

    # Depool DNS discovery records on the old dc, confd will apply the change
    dnsdisc_records.depool(args.dc_from)

    # Verify that the IP of the records matches the expected one
    for record in MEDIAWIKI_SERVICES:
        dnsdisc_records.check_record(record, '{name}.svc.{dc_to}.wmnet'.format(name=record, dc_to=args.dc_to))

    # Sleep remaining time up to DNS_SHORT_TTL to let the set_master_datacenter to propagate
    remaining = DNS_SHORT_TTL - (time.time() - start)
    if remaining > 0:
        logger.info('Sleeping %.3f seconds to reach the %d seconds mark', remaining, DNS_SHORT_TTL)
        time.sleep(remaining)
