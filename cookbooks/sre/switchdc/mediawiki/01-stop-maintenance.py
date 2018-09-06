"""Stop MediaWiki maintenance jobs"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    logger.info('Stopping MediaWiki maintenance jobs in %s', args.dc_from)
    spicerack.mediawiki().stop_cronjobs(args.dc_from)
