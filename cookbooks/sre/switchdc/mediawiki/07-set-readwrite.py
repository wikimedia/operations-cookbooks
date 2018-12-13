"""Set MediaWiki in read-write mode"""
import logging

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Set MediaWiki in read-write in %s', args.dc_to)

    mediawiki = spicerack.mediawiki()
    prefix = ''
    if args.live_test:
        prefix = '[DRY-RUN] '

    mediawiki.set_readwrite(args.dc_to)
    spicerack.irc_logger.info('%sMediaWiki read-only period ends at: %s', prefix, datetime.utcnow())
