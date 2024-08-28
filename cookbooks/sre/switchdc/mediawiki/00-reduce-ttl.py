"""Reduce TTL for various DNS Discovery entries."""

import logging
import time

from cookbooks.sre.switchdc.mediawiki import (
    DNS_SHORT_TTL,
    MEDIAWIKI_SERVICES,
    MediaWikiSwitchDCBase,
    MediaWikiSwitchDCRunnerBase,
)

logger = logging.getLogger(__name__)


class ReduceDiscoveryTTLsRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to reduce DNS Discovery record TTLs."""

    def action(self):
        """Required by base class API."""
        logger.info('Reducing DNS Discovery TTL to %d for records: %s', DNS_SHORT_TTL, MEDIAWIKI_SERVICES)
        discovery = self.spicerack.discovery(*MEDIAWIKI_SERVICES)
        old_ttl_sec = max(record.ttl for record in discovery.resolve())
        discovery.update_ttl(DNS_SHORT_TTL)
        logger.info('Sleeping for the old TTL (%d seconds) to allow the old records to expire...', old_ttl_sec)
        time.sleep(old_ttl_sec)


class ReduceDiscoveryTTLs(MediaWikiSwitchDCBase):
    """Reduce TTL for various DNS Discovery entries."""

    runner_class = ReduceDiscoveryTTLsRunner
