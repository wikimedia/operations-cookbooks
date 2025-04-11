"""Restore TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import argument_parser_base, load_services, post_process_args


logger = logging.getLogger(__name__)
SERVICES = load_services()


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __doc__, SERVICES)


def run(args, spicerack):
    """Required by spicerack API."""
    post_process_args(args)
    logger.info('Restoring DNS Discovery TTL to 300 for services: %s', ", ".join(args.services))
    discovery = spicerack.discovery(*args.services)
    discovery.update_ttl(300)
