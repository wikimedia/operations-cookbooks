"""Cookbook to perform a rolling reboot/restart of logstash on apifeatureservices hosts"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class LogstashRestartReboot(SREBatchBase):
    """Class to roll-restart or -reboot a Logstash service on apifeatureusage

    Usage example:
      cookbook sre.apifeatureusage.roll-restart-reboot-logstash \
         --reason "Rolling reboot to pick up new kernel" reboot

      cookbook sre.apifeatureusage.roll-restart-reboot-logstash \
         --reason "Rolling restart to pick new OpenSSL" restart_daemons
    """

    batch_default = 1
    grace_sleep = 120

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return LogstashRestartRebootRunner(args, self.spicerack)


class LogstashRestartRebootRunner(SREBatchRunnerBase):
    """Roll reboot/restart a Logstash cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ["apifeatureusage"]

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ["logstash"]
