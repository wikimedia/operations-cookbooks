"""Switch datacenter for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import argument_parser_base, post_process_args

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by the Spicerack API."""
    post_process_args(args)
    discovery = spicerack.discovery(*args.services)
    spicerack.irc_logger.info('Switching services %s: %s => %s', ", ".join(args.services),
                              args.dc_from, args.dc_to)
    discovery.pool(args.dc_to)
    discovery.depool(args.dc_from)
    for svc in args.services:
        discovery.check_record(svc, '{service}.svc.{dc_to}.wmnet'.format(service=svc, dc_to=args.dc_to))
