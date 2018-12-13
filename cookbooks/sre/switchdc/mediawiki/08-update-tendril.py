"""Update Tendril tree"""
import logging

from spicerack.mysql import CORE_SECTIONS

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Update Tendril tree to start from the core DB masters in %s', args.dc_to)

    mysql = spicerack.mysql()
    tendril_host = mysql.get_dbs('P{P:mariadb::misc::tendril} and A:eqiad')

    for section in CORE_SECTIONS:
        # get_core_dbs() ensure that only one host is matched
        master = mysql.get_core_dbs(datacenter=args.dc_to, replication_role='master', section=section).hosts[0]
        query = ("UPDATE shards SET master_id = (SELECT id FROM servers WHERE host = '{master}') WHERE "  # nosec
                 "name = '{section}'").format(master=master, section=section)
        tendril_host.run_query(query, database='tendril')
