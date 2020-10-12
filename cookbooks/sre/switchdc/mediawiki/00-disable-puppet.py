"""Disable Puppet where Puppet patches are required to switch datacenter"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args, PUPPET_REASON


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    remote = spicerack.remote()

    logger.info('Disabling Puppet on MediaWiki maintenance hosts in %s and %s', args.dc_from, args.dc_to)
    remote.query('A:mw-maintenance').run_sync('disable-puppet "{message}"'.format(message=PUPPET_REASON))
