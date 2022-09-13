"""WDQS/WCQS cookbook to perform rolling restart of nginx

Usage example:
    cookbook sre.wdqs.restart-nginx --alias wdqs-public \
       --reason "Rolling restart to pick up OpenSSL update" restart

"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class RestartNginx(SREBatchBase):
    """A WQS nginx restart class"""

    batch_default = 1
    valid_actions = ('restart',)
    grace_sleep = 2

    # We must implement this abstract method
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RestartNginxRunner(args, self.spicerack)


class RestartNginxRunner(SREBatchRunnerBase):
    """Roll restart/reboot a WQS cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['wcqs-public', 'wdqs-all', 'wdqs-main', 'wdqs-internal'
                'wdqs-public', 'wdqs-test']

    @property
    def restart_daemons(self):
        """Required by RebootRunnerBase"""
        return ['nginx']
