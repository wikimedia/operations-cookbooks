"""Run Puppet on all DB masters"""
import logging

from cumin import nodeset

from cookbooks.sre.switchdc.mediawiki import READ_ONLY_SERVICE_RE, argument_parser_base, post_process_args

__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    mysql = spicerack.mysql_legacy()
    hosts = mysql.get_core_dbs(replication_role="master")
    icinga_hosts = spicerack.icinga_hosts(nodeset(str(hosts)))

    logger.info('Running Puppet on all DB masters')
    spicerack.remote().query('A:db-role-master').run_sync('run-puppet-agent', batch_size=5)

    logger.info('Rechecking services on Icinga, and waiting for recovery before un-downtiming read-only checks.')
    icinga_hosts.recheck_failed_services()
    icinga_hosts.wait_for_optimal()

    logger.info('Un-downtiming read-only checks.')
    icinga_hosts.remove_service_downtimes(READ_ONLY_SERVICE_RE)
