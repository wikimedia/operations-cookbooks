"""Elasticsearch Clusters Operations"""
import argparse
import logging
from datetime import timezone

from dateutil.parser import parse

__owner_team__ = 'Data Platform'
logger = logging.getLogger(__name__)

# Used in imports for other files
CLUSTERGROUPS = ('search_eqiad', 'search_codfw', 'relforge', 'cloudelastic', 'logging-eqiad', 'logging-codfw')


# TODO: Eventually we may want to move this to a more generic place (for example, spicerack itself)
def valid_datetime_type(datetime_str):
    """Custom argparse type for user datetime values given from the command line"""
    try:
        dt = parse(datetime_str)
        if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError as e:
        msg = "Error reading datetime ({0})!".format(datetime_str)
        raise argparse.ArgumentTypeError(msg) from e
