"""Reduce TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, DNS_SHORT_TTL, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    records = ('api-ro', 'api-rw', 'appservers-ro', 'appservers-rw', 'jobrunner', 'videoscaler', 'parsoid-php')
    logger.info('Reducing DNS Discovery TTL to %d for records: %s', DNS_SHORT_TTL, records)
    discovery = spicerack.discovery(*records)
    discovery.update_ttl(DNS_SHORT_TTL)
    # TODO: add sleep for previous TTL, skipped for now because the warmup step is longer than that
