"""Set MediaWiki in read-write mode"""
import logging

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Set MediaWiki in read-write in %s', args.dc_to)

    mediawiki = spicerack.mediawiki()
    mediawiki.set_readwrite(args.dc_to)
    spicerack.irc_logger.info('MediaWiki read-only period ends at: %s', datetime.utcnow())
