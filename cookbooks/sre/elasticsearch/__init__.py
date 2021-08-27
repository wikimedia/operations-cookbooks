"""Elasticsearch Clusters Operations"""
import argparse
import logging

from dateutil.parser import parse

__title__ = __doc__
logger = logging.getLogger(__name__)

CLUSTERGROUPS = ('search_eqiad', 'search_codfw', 'relforge', 'cloudelastic')  # Used in imports for other files


# TODO: Eventually we may want to move this to a more generic place (for example, spicerack itself)
def valid_datetime_type(datetime_str):
    """Custom argparse type for user datetime values given from the command line"""
    try:
        dt = parse(datetime_str)
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            raise argparse.ArgumentTypeError('datetime should be naive (without timezone information)')
        return dt
    except ValueError as e:
        msg = "Error reading datetime ({0})!".format(datetime_str)
        raise argparse.ArgumentTypeError(msg) from e
