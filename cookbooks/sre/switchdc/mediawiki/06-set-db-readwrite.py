"""Set the new-site core DB primaries in read-write mode."""

import logging

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class SetDBReadWriteRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to set the new-site core DB primaries in read-write."""

    def run(self):
        """Required by Spicerack API."""
        logger.info('Setting in read-write mode all the core DB primaries in %s', self.dc_to)
        mysql = self.spicerack.mysql_legacy()
        mysql.set_core_masters_readwrite(self.dc_to)


class SetDBReadWrite(MediaWikiSwitchDCBase):
    """Set the new-site core DB primaries in read-write mode."""

    runner_class = SetDBReadWriteRunner
