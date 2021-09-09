"""Reduce TTL for various DNS Discovery entries"""
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

    logger.info('Reducing DNS Discovery TTL to %d for records: %s', DNS_SHORT_TTL, MEDIAWIKI_SERVICES)
    discovery = spicerack.discovery(*MEDIAWIKI_SERVICES)
    old_ttl_sec = max(record.ttl for record in discovery.resolve())
    discovery.update_ttl(DNS_SHORT_TTL)
    logger.info('Sleeping for the old TTL (%d seconds) to allow the old records to expire...', old_ttl_sec)
    time.sleep(old_ttl_sec)
