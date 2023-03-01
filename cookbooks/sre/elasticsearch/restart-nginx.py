"""Roll-restart nginx on Elastic clusters"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class ElasticRestartNginx(SREBatchBase):
    """cookbook to perform rolling restart of nginx on Elastic

    Usage example:
        cookbook sre.elasticsearch.restart-nginx --alias relforge \
           --reason "Rolling restart to pick up OpenSSL update" restart

    """

    batch_default = 1
    valid_actions = ('restart_daemons',)
    grace_sleep = 2

    # We must implement this abstract method
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ElasticRestartNginxRunner(args, self.spicerack)


class ElasticRestartNginxRunner(SREBatchRunnerBase):
    """Roll restart Nginx on an Elastic cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ['cloudelastic', 'elastic', 'elastic-canary', 'elastic-codfw',
                'elastic-eqiad', 'relforge']

    @property
    def restart_daemons(self):
        """Required by RebootRunnerBase"""
        return ['nginx']
