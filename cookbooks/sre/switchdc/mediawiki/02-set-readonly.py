"""Set MediaWiki in read-only mode"""
import logging
import time

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
    logger.info('Set MediaWiki in read-only in %s and %s', args.dc_from, args.dc_to)

    if args.live_test:
        prefix = '[DRY-RUN] '
    else:
        prefix = ''

    mediawiki = spicerack.mediawiki()
    spicerack.sal_logger.info('%sMediaWiki read-only period starts at: %s', prefix, datetime.utcnow())
    for dc in (args.dc_to, args.dc_from):
        if args.live_test and dc is args.dc_to:
            logger.info('Skip setting MediaWiki read-only in %s', dc)
            continue
        mediawiki.set_readonly(dc, args.ro_reason)

    logger.info('Sleeping 10s to allow in-flight requests to complete')
    time.sleep(10)
