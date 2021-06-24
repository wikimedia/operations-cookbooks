"""Stop MediaWiki maintenance jobs"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    datacenters = [args.dc_from]
    if args.live_test:
        logger.info("Skipping disable of maintenance jobs in %s (active DC)", args.dc_to)
    else:
        datacenters.append(args.dc_to)
    logger.info('Stopping MediaWiki maintenance jobs in %s', ', '.join(datacenters))
    for datacenter in datacenters:
        spicerack.mediawiki().stop_periodic_jobs(datacenter)
