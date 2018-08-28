"""Disable Puppet where Puppet patches are required to switch datacenter"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args, PUPPET_REASON


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    remote = spicerack.remote()

    logger.info('Disabling Puppet on MediaWiki mainteance hosts in %s and %s', args.dc_from, args.dc_to)
    remote.query('A:mw-maintenance').run_sync('disable-puppet "{message}"'.format(message=PUPPET_REASON))

    logger.info('Disabling Puppet on text caches in %s and %s', args.dc_from, args.dc_to)
    target = remote.query('A:cp-text and (A:cp-{dc_from} or A:cp-{dc_to})'.format(
        dc_from=args.dc_from, dc_to=args.dc_to))
    target.run_sync('disable-puppet "{message}"'.format(message=PUPPET_REASON))
    logger.info('The puppet changes for text caches can be now merged.')
