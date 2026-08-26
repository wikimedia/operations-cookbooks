"""Cookbook to restart/reboot URL downloaders."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class UrlDownloadersRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of URL downloaders

    Usage example:
        cookbook sre.misc-clusters.roll-restart-reboot-urldownloaders \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.misc-clusters.roll-restart-reboot-urldownloaders \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    owner_team = 'Infrastructure Foundations'
    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return UrlDownloadersRestartRebootRunner(args, self.spicerack)


class UrlDownloadersRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an Url downloaders cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['url-downloader', 'url-downloader-eqiad', 'url-downloader-codfw']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['squid']
