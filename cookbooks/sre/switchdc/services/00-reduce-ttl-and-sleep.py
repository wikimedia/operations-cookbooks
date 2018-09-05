"""Reduce TTL for various DNS Discovery entries"""
import logging
import time

from cookbooks.sre.switchdc.services import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Reducing DNS Discovery TTL to 10 for records: %s', ", ".join(args.services))
    discovery = spicerack.discovery(*args.services)
    discovery.update_ttl(10)
    logger.info('Now waiting for the DNS TTL to expire for all records. Please be patient.')
    time.sleep(5)
    logger.info('Yes, that is 5 minutes. Blame Joe.')
    time.sleep(295)
