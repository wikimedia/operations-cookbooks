"""Set the core DB masters in read-only mode and check replication"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    logger.info('Setting in read-only mode all the core DB masters in %s and verify those in %s',
                args.dc_from, args.dc_to)
    mysql = spicerack.mysql_legacy()
    if args.live_test:
        logger.info('Skip verifying core DB masters in %s are in read-only mode', args.dc_to)
    else:
        mysql.verify_core_masters_readonly(args.dc_to, True)

    mysql.set_core_masters_readonly(args.dc_from)

    logger.info('Check that all core masters in %s are in sync with the core masters in %s.', args.dc_to, args.dc_from)
    mysql.check_core_masters_in_sync(args.dc_from, args.dc_to)
