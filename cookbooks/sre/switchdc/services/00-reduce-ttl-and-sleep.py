"""Reduce TTL for various DNS Discovery entries"""
import logging
import time

from cookbooks.sre.switchdc.services import argument_parser_base, load_services, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)
SERVICES = load_services()


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__, SERVICES)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Reducing DNS Discovery TTL to 10 for records: %s', ", ".join(args.services))
    discovery = spicerack.discovery(*args.services)
    discovery.update_ttl(10)
    logger.info('Now waiting for the DNS TTL to expire for all records. Please be patient.')
    time.sleep(5)
    logger.info('Yes, that is 5 minutes. Blame Joe.')
    time.sleep(295)
