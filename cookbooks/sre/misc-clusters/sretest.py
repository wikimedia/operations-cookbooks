"""sretest reboot cookbook

Usage example:
    cookbook sre.misc-clusters.sretest

"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class Reboot(SREBatchBase):
    """An sretest reboot class"""

    # We must implement this abstract method
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return BatchRunner(args, self.spicerack)


class BatchRunner(SREBatchRunnerBase):
    """An example reboot class"""

    @property
    def allowed_aliases(self):
        """Required by RebootRunnerBase"""
        return ['sretest']

    @property
    def restart_daemons(self):
        """Required by RebootRunnerBase"""
        # sretest dosn't  have real daemons to restart, nrpe is provided as an example
        return ['nagios-nrpe-server.service']

    @property
    def pre_scripts(self):
        """Add depool to the list of prescripts"""
        return ['/usr/local/bin/puppet-enabled']

    @property
    def post_scripts(self):
        """Add pool to the list of prescripts"""
        return ['/usr/local/bin/puppet-enabled']
