"""Downtime read-only checks on MariaDB primaries changed in Phase 3 so they don't page."""
import datetime
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

    logger.info("Downtiming read-only checks on MariaDB primaries in both DCs.")
    if args.live_test:
        reason = spicerack.admin_reason("MediaWiki DC switchover live test")
    else:
        reason = spicerack.admin_reason("MediaWiki DC switchover")
    # We'll delete the downtime in 09-run-puppet-on-db-masters, but set a six-hour duration in case that's skipped.
    icinga_hosts.downtime_services(READ_ONLY_SERVICE_RE, reason=reason, duration=datetime.timedelta(hours=6))
