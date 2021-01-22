"""thumbor reboot cookbook

Usage example:
    cookbook sre.misc-clusters.thumbor

"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class Reboot(SREBatchBase):
    """An thumbor reboot class"""

    batch_default = 1

    # We must implement this abstract method
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return BatchRunner(args, self.spicerack)


class BatchRunner(SREBatchRunnerBase):
    """Thumbor reboot class"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['thumbor', 'thumbor-canary', 'thumbor-codfw', 'thumbor-eqiad']

    @property
    def restart_daemons(self):
        """Required by RebootRunnerBase"""
        return ['thumbor-instances']

    @property
    def pre_scripts(self):
        """Add depool to the list of prescripts"""
        return ['/usr/local/bin/depool']

    @property
    def post_scripts(self):
        """Add pool to the list of prescripts"""
        return ['/usr/local/bin/pool']
