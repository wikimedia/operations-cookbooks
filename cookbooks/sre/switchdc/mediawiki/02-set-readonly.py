"""Set MediaWiki in read-only mode."""

import logging
import time

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class SetReadOnlyRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to set MediaWiki in read-only mode."""

    def action(self):
        """Required by base class API."""
        logger.info('Set MediaWiki in read-only in %s and %s', self.dc_from, self.dc_to)

        if self.live_test:
            prefix = '[DRY-RUN] '
        else:
            prefix = ''

        mediawiki = self.spicerack.mediawiki()
        message = f'{prefix}MediaWiki read-only period starts at: {datetime.utcnow()}'
        self.spicerack.sal_logger.info(message)
        self.update_task(message)
        for dc in (self.dc_to, self.dc_from):
            if self.live_test and dc is self.dc_to:
                logger.info('Skip setting MediaWiki read-only in %s', dc)
                continue
            mediawiki.set_readonly(dc, self.ro_reason)

        logger.info('Sleeping 10s to allow in-flight requests to complete')
        time.sleep(10)


class SetReadOnly(MediaWikiSwitchDCBase):
    """Set MediaWiki in read-only mode."""

    runner_class = SetReadOnlyRunner
