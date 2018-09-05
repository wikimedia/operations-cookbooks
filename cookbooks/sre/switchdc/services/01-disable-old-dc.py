"""Disable datacenter for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import parse_args

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by the Spicerack API."""
    args = parse_args(__name__, __title__, args)
    discovery = spicerack.discovery(*args.services)
    discovery.depool(args.dc_from)
    for svc in args.services:
        discovery.check_record(svc, '{service}.svc.{dc_to}'.format(service=svc, dc_to=args.dc_to))
