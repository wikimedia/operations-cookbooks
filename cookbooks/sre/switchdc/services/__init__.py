"""Switch Datacenter for Services"""
import argparse

from wmflib.config import load_yaml_config
from wmflib.constants import CORE_DATACENTERS

from cookbooks.sre.discovery.datacenter import EXCLUDED_SERVICES
from cookbooks.sre.switchdc.mediawiki import MEDIAWIKI_SERVICES, MEDIAWIKI_RO_SERVICES


def load_services():
    """Load the dc-local hostnames for all active-active services."""
    # TODO: find a way to use spicerack.config_dir here
    # It's not easy as we need this when parsing CLI arguments.
    config_full_path = '/etc/spicerack/cookbooks/sre.switchdc.services.yaml'
    every_service = load_yaml_config(config_full_path)
    # Only select active-active services
    services = {}
    for srv, data in every_service.items():
        if data['active_active']:
            services[srv] = data['rec']
    return services


def argument_parser_base(name, title, services):
    """Parse the command line arguments for all the sre.switchdc.services cookbooks."""
    mw_services = set(MEDIAWIKI_SERVICES) | set(MEDIAWIKI_RO_SERVICES)
    all_services = set(services.keys()) - set(EXCLUDED_SERVICES.keys()) - mw_services
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--services', metavar='SERVICES', choices=all_services, nargs="+",
                        help='Names of the services to switch; if left blank, all services '
                        'will be switched over.', default=all_services)
    parser.add_argument('--exclude', metavar="EXCLUDED_SERVICES", choices=all_services, nargs="+",
                        help='Names of the services that will NOT be switched over, if any.')
    parser.add_argument('dc_from', metavar='DC_FROM', choices=CORE_DATACENTERS,
                        help='Name of the datacenter to switch away from. One of: %(choices)s.')
    parser.add_argument('dc_to', metavar='DC_TO', choices=CORE_DATACENTERS,
                        help='Name of the datacenter to switch to. One of: %(choices)s.')
    return parser


def post_process_args(args):
    """Do any post-processing of the parsed arguments."""
    # Mangle the services list removing the excluded ones
    if args.exclude is not None:
        actual_services = list(set(args.services) - set(args.exclude))
        args.services = actual_services
