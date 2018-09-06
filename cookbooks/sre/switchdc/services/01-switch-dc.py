"""Switch datacenter for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import parse_args

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by the Spicerack API."""
    args = parse_args(__name__, __title__, args)
    discovery = spicerack.discovery(*args.services)
    spicerack.irc_logger.info('Switching services %s: %s => %s', ", ".join(args.services),
                              args.dc_from, args.dc_to)
    discovery.pool(args.dc_to)
    discovery.depool(args.dc_from)
    for svc in args.services:
        discovery.check_record(svc, '{service}.svc.{dc_to}.wmnet'.format(service=svc, dc_to=args.dc_to))
