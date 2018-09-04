"""Start MediaWiki mainteance jobs"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args, PUPPET_REASON


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    logger.info('Starting MediaWiki mainteance jobs in %s', args.dc_to)

    mw_maintenance = spicerack.remote().query('A:mw-maintenance')
    mw_maintenance.run_sync('run-puppet-agent --enable "{message}"'.format(message=PUPPET_REASON))

    mediawiki = spicerack.mediawiki()
    mediawiki.check_cronjobs_enabled(args.dc_to)
    mediawiki.check_cronjobs_disabled(args.dc_from)
