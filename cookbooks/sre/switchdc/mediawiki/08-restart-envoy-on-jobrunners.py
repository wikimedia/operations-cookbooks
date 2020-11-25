"""Restart Envoy on jobrunners so changeprop re-resolves the jobrunner service name for its long-running connections."""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = 'Restart Envoy on jobrunners in DC_FROM.'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Restarting Envoy on jobrunners in %s', args.dc_from)
    spicerack.remote().query('A:mw-jobrunner-{dc}'.format(dc=args.dc_from)).run_sync('systemctl restart envoyproxy')
