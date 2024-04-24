"""Cookbook to perform a rolling reboot/restart of Logstash collector nodes"""

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class LogstashCollectorsRestartReboot(SREBatchBase):
    """Class to roll-restart or -reboot a Logstash collectors cluster

    When restarting daemons, both Apache and the logstash service get restarted

    Usage example:
      cookbook sre.o11y.roll-restart-reboot-logstash-collectors \
         --reason "Rolling reboot to pick up new kernel" reboot

      cookbook sre.o11y.roll-restart-reboot-logstash-collectors \
         --reason "Rolling restart to pick new OpenSSL" restart_daemons
    """

    batch_default = 1
    grace_sleep = 5

    # Required
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return LogstashCollectorsRestartRebootRunner(args, self.spicerack)


class LogstashCollectorsRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart a Logstash collectors cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['logstash-collector']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['apache2', 'logstash', 'opensearch-dashboards', 'envoyproxy']
