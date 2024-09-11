"""Set the old-site core DB primaries in read-only mode and check replication."""

import logging

from spicerack.mysql_legacy import MysqlLegacyError

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class SetDBReadOnlyRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to set the old-site core DB primaries in read-only."""

    def action(self):
        """Required by base class API."""
        logger.info('Setting in read-only mode all the core DB primaries in %s and verify those in %s',
                    self.dc_from, self.dc_to)
        mysql = self.spicerack.mysql_legacy()
        if self.live_test:
            logger.info('Skip verifying core DB primaries in %s are in read-only mode', self.dc_to)
        else:
            mysql.verify_core_masters_readonly(self.dc_to, True)

        mysql.set_core_masters_readonly(self.dc_from)

        logger.info('Check that all core primaries in %s are in sync with the core primaries in %s.',
                    self.dc_to, self.dc_from)
        try:
            mysql.check_core_masters_in_sync(self.dc_from, self.dc_to)
        except MysqlLegacyError as e:
            if self.live_test:
                logger.warning(
                    'Check failed while in live-test mode (%s). This is expected if circular replication is not '
                    '(yet) configured.',
                    e,
                )
            else:
                raise


class SetDBReadOnly(MediaWikiSwitchDCBase):
    """Set the old-site core DB primaries in read-only mode and check replication."""

    runner_class = SetDBReadOnlyRunner
