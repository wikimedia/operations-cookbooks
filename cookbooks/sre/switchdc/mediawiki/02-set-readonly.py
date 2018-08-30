"""Set MediaWiki in read-only mode"""
import logging

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Set MediaWiki in read-only in %s and %s', args.dc_from, args.dc_to)

    mediawiki = spicerack.mediawiki()
    mediawiki.set_readonly(args.dc_to, args.ro_reason)

    spicerack.irc_logger.info('MediaWiki read-only period starts at: %s', datetime.utcnow())
    mediawiki.set_readonly(args.dc_from, args.ro_reason)
