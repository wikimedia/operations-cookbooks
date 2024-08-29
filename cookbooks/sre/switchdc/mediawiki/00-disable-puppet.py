"""Disable Puppet on maintenance hosts so that it doesn't restart stopped jobs."""

import logging

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class DisablePuppetRunner(MediaWikiSwitchDCRunnerBase):
    """Runner to disable puppet on maintenance hosts."""

    def action(self):
        """Required by base class API."""
        remote = self.spicerack.remote()
        logger.info('Disabling Puppet on MediaWiki maintenance hosts in %s and %s', self.dc_from, self.dc_to)
        remote.query('A:mw-maintenance').run_sync(f'disable-puppet "{self.reason}"')


class DisablePuppet(MediaWikiSwitchDCBase):
    """Disable Puppet on maintenance hosts so that it doesn't restart stopped jobs."""

    runner_class = DisablePuppetRunner
