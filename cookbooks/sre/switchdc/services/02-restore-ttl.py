"""Reduce TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Restoring DNS Discovery TTL to 300 for services: %s', ", ".join(args.services))
    discovery = spicerack.discovery(*args.services)
    discovery.update_ttl(300)
