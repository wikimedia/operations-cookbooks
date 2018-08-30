"""Switch Datacenter for MediaWiki"""
import argparse

from spicerack.constants import CORE_DATACENTERS


__title__ = __doc__
PUPPET_REASON = __name__
DEFAULT_READ_ONLY_REASON = 'MediaWiki is in read-only mode for maintenance. Please try again in a few minutes.'


def parse_args(name, title, args):
    """Parse the command line arguments for all the sre.switchdc.mediawiki cookbooks."""
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--ro-reason', default=DEFAULT_READ_ONLY_REASON,
                        help='The read-only reason message to set in Conftool.')
    parser.add_argument('--live-test', action='store_true',
                        help=('Perform a live test assuming that DC_TO is already the active datacenter and DC_FROM is '
                              'already the passive datacenter. Automatically skip or invert, when feasible, the steps '
                              'that will disrupt DC_TO if they were run.'))
    parser.add_argument('dc_from', metavar='DC_FROM', choices=CORE_DATACENTERS,
                        help='Name of the datacenter to switch away from. One of: %(choices)s.')
    parser.add_argument('dc_to', metavar='DC_TO', choices=CORE_DATACENTERS,
                        help='Name of the datacenter to switch to. One of: %(choices)s.')

    if args.dc_from == args.dc_to:
        parser.error('--dc-from and --dc-to must differ')

    return parser.parse_args(args=args)
