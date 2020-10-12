"""Start MediaWiki maintenance jobs"""
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
    logger.info('Starting MediaWiki maintenance jobs in %s', args.dc_to)

    mw_maintenance = spicerack.remote().query('A:mw-maintenance')
    mw_maintenance.run_sync('run-puppet-agent --enable "{message}"'.format(message=PUPPET_REASON))

    mediawiki = spicerack.mediawiki()
    mediawiki.check_cronjobs_enabled(args.dc_to)
    mediawiki.check_cronjobs_disabled(args.dc_from)
