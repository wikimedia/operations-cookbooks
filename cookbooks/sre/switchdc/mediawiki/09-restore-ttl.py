"""Restore TTL for various DNS Discovery entries."""

import logging

from spicerack.remote import RemoteExecutionError

from cookbooks.sre.switchdc.mediawiki import MEDIAWIKI_SERVICES, MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class RestoreDiscoveryTTLsRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to restore DNS Discovery record TTLs."""

    def action(self):
        """Required by base class API."""
        logger.info('Restoring DNS Discovery TTL to 300 for records: %s', MEDIAWIKI_SERVICES)
        dnsdisc_records = self.spicerack.discovery(*MEDIAWIKI_SERVICES)
        dnsdisc_records.update_ttl(300)

        logger.info('Removing stale confd files generated when switching discovery records')
        command = 'rm -fv /var/run/confd-template/_var_lib_gdnsd_discovery-{{{records}}}.state.err'.format(
            records=','.join(MEDIAWIKI_SERVICES))

        # As authdns hosts could be depooled and under maintenance but still receiving confd updates and hence
        # generating the error files, attempt to delete them best-effort, just logging in case of failure.
        try:
            self.spicerack.remote().query('A:dnsbox').run_sync(command)
        except RemoteExecutionError:
            logger.warning(
                "Confd templates error files not properly cleared, check the output above for failures. "
                "Check if any dnsauth host was unreachable or under maintenance."
            )


class RestoreDiscoveryTTLs(MediaWikiSwitchDCBase):
    """Restore TTL for various DNS Discovery entries."""

    runner_class = RestoreDiscoveryTTLsRunner
