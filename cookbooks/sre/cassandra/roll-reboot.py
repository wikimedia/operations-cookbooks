"""Cassandra reboot cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class CassandraReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot of a Cassandra cluster

    Usage example:
        cookbook sre.cassandra.roll-reboot \
           --alias ml-cache-eqiad \
           --reason "Rolling reboot to pick up new kernel" reboot

    """

    batch_default = 1
    grace_sleep = 300
    valid_actions = ('reboot',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return CassandraRebootRunner(args, self.spicerack)


class CassandraRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot a Cassandra cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['ml-cache', 'ml-cache-eqiad', 'ml-cache-codfw',
                'cassandra-dev']

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cassandra-dev or A:ml-cache'
