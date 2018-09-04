"""Set the core DB masters in read-write mode"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    logger.info('Setting in read-write mode all the core DB masters in %s', args.dc_to)
    mysql = spicerack.mysql()
    mysql.set_core_masters_readwrite(args.dc_to)
