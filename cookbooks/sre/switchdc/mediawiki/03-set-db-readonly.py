"""Set the core DB masters in read-only mode and check replication"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    logger.info('Setting in read-only mode all the core DB masters in %s and verify those in %s',
                args.dc_from, args.dc_to)
    mysql = spicerack.mysql()
    mysql.verify_core_masters_readonly(args.dc_to, True)
    mysql.set_core_masters_readonly(args.dc_from)

    logger.info('Check that all core masters in %s are in sync with the core masters in %s.', args.dc_to, args.dc_from)
    mysql.check_core_masters_in_sync(args.dc_from, args.dc_to)
