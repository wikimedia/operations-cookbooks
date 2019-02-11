"""Elasticsearch Clusters Operations"""
import argparse

from datetime import datetime

from dateutil.parser import parse


__title__ = __doc__
CLUSTERGROUPS = ('search_eqiad', 'search_codfw', 'relforge')


def valid_datetime_type(datetime_str):
    """Custom argparse type for user datetime values given from the command line"""
    try:
        return parse(datetime_str)
    except ValueError:
        msg = "Error reading datetime ({0})!".format(datetime_str)
        raise argparse.ArgumentTypeError(msg)


def argument_parser_base(name, title):
    """Parse the command line arguments for all the sre.elasticsearch cookbooks.

    Todo:
        Remove ``--nodes-has-lvs`` for a better implementation as this was introduced because
        relforge cluster does not have lvs enabled.
    """
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    parser.add_argument('admin_reason', help='Administrative Reason')
    parser.add_argument('--start-datetime', type=valid_datetime_type,
                        help='start datetime in ISO 8601 format e.g 2018-09-15T15:53:00+00:00')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--nodes-per-run', default=3, type=int, help='Number of nodes per run.')
    parser.add_argument('--nodes-has-lvs', default=True, type=bool, help='True = has lvs, False = no lvs')

    return parser


def post_process_args(args):
    """Do any post-processing of the parsed arguments."""
    if args.start_datetime is None:
        args.start_datetime = datetime.utcnow()
