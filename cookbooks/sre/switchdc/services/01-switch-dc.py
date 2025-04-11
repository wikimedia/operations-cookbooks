"""Switch datacenter for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.services import argument_parser_base, load_services, post_process_args


logger = logging.getLogger(__name__)
SERVICES = load_services()


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __doc__, SERVICES)


def run(args, spicerack):
    """Required by the Spicerack API."""
    post_process_args(args)
    discovery = spicerack.discovery(*args.services)
    spicerack.sal_logger.info('Switching services %s: %s => %s', ", ".join(args.services),
                              args.dc_from, args.dc_to)
    discovery.pool(args.dc_to)
    discovery.depool(args.dc_from)
    for svc in args.services:
        host_to_check = SERVICES[svc][args.dc_to]
        discovery.check_record(svc, host_to_check)
