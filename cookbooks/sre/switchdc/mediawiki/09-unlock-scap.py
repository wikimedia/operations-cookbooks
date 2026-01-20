"""Unlock scap after the switchover."""

import logging

from cookbooks.sre.deploy import DEPLOYMENT_CNAME

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)


class UnlockScapRunner(MediaWikiSwitchDCRunnerBase):
    """Runner to unlock scap."""

    def action(self):
        """Required by base class API."""
        deployment_host = self.spicerack.remote().query(self.spicerack.dns().resolve_cname(DEPLOYMENT_CNAME))

        logger.info("Unlocking scap")
        deployment_host.run_sync(
            f"runuser -u {self.spicerack.username} -- /usr/bin/scap lock --unlock-all --yes 'Datacenter "
            f"switchover from {self.dc_from} to {self.dc_to} - {self.task_id}'",
            print_progress_bars=False
        )
        logger.info("scap has been unlocked")


class UnlockScap(MediaWikiSwitchDCBase):
    """Unlock scap."""

    runner_class = UnlockScapRunner
