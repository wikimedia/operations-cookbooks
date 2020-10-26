"""Start MediaWiki maintenance jobs"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Running Puppet on all DB masters')
    spicerack.remote().query('A:db-role-master').run_sync('run-puppet-agent', batch_size=5)
