"""Reduce TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    records = ('api-rw', 'appservers-rw', 'jobrunner', 'videoscaler')
    logger.info('Reducing DNS Discovery TTL to 10 for records: %s', records)
    discovery = spicerack.discovery(*records)
    discovery.update_ttl(10)
