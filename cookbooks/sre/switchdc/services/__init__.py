"""Switch Datacenter for Services"""
import argparse

from spicerack.constants import CORE_DATACENTERS

__title__ = __doc__

CORE_SERVICES = ('parsoid', 'restbase', 'restbase-async', 'mobileapps')
OTHER_SERVICES = ('apertium', 'citoid', 'cxserver', 'eventstreams', 'graphoid', 'mathoid', 'proton',
                  'pdfrender', 'recommendation-api', 'zotero')
MEDIAWIKI_RELATED_SERVICES = ('eventbus', 'ores', 'wdqs', 'wdqs-internal')
ALL_SERVICES = CORE_SERVICES + OTHER_SERVICES + MEDIAWIKI_RELATED_SERVICES


def argument_parser_base(name, title):
    """Parse the command line arguments for all the sre.switchdc.services cookbooks."""
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--services', metavar='SERVICES', choices=ALL_SERVICES, nargs="+",
                        help='Names of the services to switch; if left blank, all services '
                        'will be switched over.', default=ALL_SERVICES)
    parser.add_argument('--exclude', metavar="EXCLUDED_SERVICES", choices=ALL_SERVICES, nargs="+",
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
