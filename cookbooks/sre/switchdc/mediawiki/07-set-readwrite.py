"""Set MediaWiki in read-write mode"""
import logging

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    mediawiki = spicerack.mediawiki()
    prefix = ''
    if args.live_test:
        prefix = '[DRY-RUN] '

    for dc in (args.dc_to, args.dc_from):
        logger.info('Set MediaWiki in read-write in %s', dc)
        mediawiki.set_readwrite(dc)

    spicerack.sal_logger.info('%sMediaWiki read-only period ends at: %s', prefix, datetime.utcnow())
