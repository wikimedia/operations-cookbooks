"""Set MediaWiki in read-only mode"""
import logging
import time

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Set MediaWiki in read-only in %s and %s', args.dc_from, args.dc_to)

    mediawiki = spicerack.mediawiki()
    if args.live_test:
        logger.info('Skip setting MediaWiki read-only in %s', args.dc_to)
        prefix = '[DRY-RUN] '
    else:
        mediawiki.set_readonly(args.dc_to, args.ro_reason)
        prefix = ''

    spicerack.irc_logger.info('%sMediaWiki read-only period starts at: %s', prefix, datetime.utcnow())
    mediawiki.set_readonly(args.dc_from, args.ro_reason)

    logger.info('Sleeping 10s to allow in-flight requests to complete')
    time.sleep(10)
