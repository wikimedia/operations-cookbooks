"""thumbor reboot cookbook

Usage example:
    cookbook sre.misc-clusters.thumbor --alias thumbor-codfw \
       --reason "Rolling reboot to pick up new kernel" reboot

"""

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class Reboot(SREBatchBase):
    """An thumbor reboot class"""

    batch_default = 1

    # We must implement this abstract method
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return BatchRunner(args, self.spicerack)


class BatchRunner(SRELBBatchRunnerBase):
    """Roll restart/reboot a Thumbor cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['thumbor', 'thumbor-canary', 'thumbor-codfw', 'thumbor-eqiad']

    @property
    def restart_daemons(self):
        """Required by RebootRunnerBase"""
        return ['thumbor-instances']
