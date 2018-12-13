"""Set the core DB masters in read-write mode"""
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

    logger.info('Setting in read-write mode all the core DB masters in %s', args.dc_to)
    mysql = spicerack.mysql()
    mysql.set_core_masters_readwrite(args.dc_to)
