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

    logger.info('Stopping MediaWiki maintenance jobs in %s', args.dc_from)
    spicerack.mediawiki().stop_cronjobs(args.dc_from)
