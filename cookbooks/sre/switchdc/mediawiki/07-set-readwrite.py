"""Set MediaWiki in read-write mode."""

import logging

from datetime import datetime

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class SetReadWriteRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to set MediaWiki in read-write mode."""

    def action(self):
        """Required by base class API."""
        mediawiki = self.spicerack.mediawiki()
        prefix = ''
        if self.live_test:
            prefix = '[DRY-RUN] '

        for dc in (self.dc_to, self.dc_from):
            logger.info('Set MediaWiki in read-write in %s', dc)
            mediawiki.set_readwrite(dc)

        self.spicerack.sal_logger.info('%sMediaWiki read-only period ends at: %s', prefix, datetime.utcnow())


class SetReadWrite(MediaWikiSwitchDCBase):
    """Set MediaWiki in read-write mode."""

    runner_class = SetReadWriteRunner
