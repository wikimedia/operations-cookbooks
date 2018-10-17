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


def parse_args(name, title, args):
    """Parse the command line arguments for all the sre.elasticsearch cookbooks."""
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    parser.add_argument('admin_reason', help='Administrative Reason')
    parser.add_argument('--start_datetime', type=valid_datetime_type,
                        help='start datetime in ISO 8601 format e.g 2018-09-15T15:53:00+00:00')
    parser.add_argument('--task_id', help='task_id for the change')
    parser.add_argument('--nodes_per_run', default=3, type=int, help='Number of nodes per run.')

    parsed_args = parser.parse_args(args=args)

    if parsed_args.start_datetime is None:
        parsed_args.start_datetime = datetime.utcnow()
    return parsed_args
