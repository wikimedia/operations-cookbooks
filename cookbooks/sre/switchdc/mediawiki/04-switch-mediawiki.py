"""Switch MediaWiki active datacenter."""

import logging
import time

from cookbooks.sre.switchdc.mediawiki import (
    DNS_SHORT_TTL,
    MEDIAWIKI_SERVICES,
    MediaWikiSwitchDCBase,
    MediaWikiSwitchDCRunnerBase,
)

logger = logging.getLogger(__name__)


class SwitchMediaWikiRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to switch MediaWiki active datacenter."""

    def run(self):
        """Required by Spicerack API."""
        logger.info('Switch MediaWiki active datacenter to %s', self.dc_to)

        dnsdisc_records = self.spicerack.discovery(*MEDIAWIKI_SERVICES)
        mediawiki = self.spicerack.mediawiki()

        # Pool DNS discovery records on the new dc.
        # This will NOT trigger confd to change the DNS admin state as it will cause a validation error
        dnsdisc_records.pool(self.dc_to)

        # Switch MediaWiki primary/active datacenter
        start = time.time()
        mediawiki.set_master_datacenter(self.dc_to)

        # Depool DNS discovery records on the old dc, confd will apply the change
        dnsdisc_records.depool(self.dc_from)

        # Verify that the IP of the records matches the expected one
        for record in MEDIAWIKI_SERVICES:
            dnsdisc_records.check_record(record, '{name}.svc.{dc_to}.wmnet'.format(name=record, dc_to=self.dc_to))

        # Sleep remaining time up to DNS_SHORT_TTL to let the set_master_datacenter to propagate
        remaining = DNS_SHORT_TTL - (time.time() - start)
        if remaining > 0:
            logger.info('Sleeping %.3f seconds to reach the %d seconds mark', remaining, DNS_SHORT_TTL)
            time.sleep(remaining)


class SwitchMediaWiki(MediaWikiSwitchDCBase):
    """Switch MediaWiki active datacenter."""

    runner_class = SwitchMediaWikiRunner
