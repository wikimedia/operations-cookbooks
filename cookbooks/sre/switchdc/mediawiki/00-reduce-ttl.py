"""Reduce TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    records = ('api-rw', 'appservers-rw', 'jobrunner', 'videoscaler')
    logger.info('Reducing DNS Discovery TTL to 10 for records: %s', records)
    discovery = spicerack.discovery(*records)
    discovery.update_ttl(10)
