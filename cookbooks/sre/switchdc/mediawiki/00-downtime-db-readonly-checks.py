"""Downtime read-only checks on MariaDB primaries changed in Phase 3 so they don't page."""

import datetime
import logging

from cumin import nodeset

from cookbooks.sre.switchdc.mediawiki import READ_ONLY_SERVICE_RE, MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class DowntimeReadOnlyChecksRunner(MediaWikiSwitchDCRunnerBase):
    """Runner to downtime read-only checks on MariaDB primaries."""

    def action(self):
        """Required by base class API."""
        mysql = self.spicerack.mysql_legacy()
        hosts = mysql.get_core_dbs(replication_role="master")
        icinga_hosts = self.spicerack.icinga_hosts(nodeset(str(hosts)))

        logger.info("Downtiming read-only checks on MariaDB primaries in both DCs.")
        # We'll delete the downtime in 09-run-puppet-on-db-masters, but set a six-hour duration in case that's skipped.
        icinga_hosts.downtime_services(READ_ONLY_SERVICE_RE, reason=self.reason, duration=datetime.timedelta(hours=6))


class DowntimeReadOnlyChecks(MediaWikiSwitchDCBase):
    """Downtime read-only checks on MariaDB primaries changed in Phase 3 so they don't page."""

    runner_class = DowntimeReadOnlyChecksRunner
