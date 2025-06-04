"""Rolling restart of ntpsec.service on the DNS hosts identified by A:dnsbox."""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class NTPRollRestart(SREBatchBase):
    """Rolling restart of ntpsec.service on the DNS hosts.

    This cookbook is for the rolling restarts of ntpsec.service on the DNS
    hosts.  Since Puppet no longer manages the restarts for us (intentionally),
    this cookbook helps us do that and sets sane automatic defaults for the
    batches and sleep intervals.

    Note that there is an alert in place for ntp.conf: if the file is modified
    and ntpsec.service is not restarted to pick up the changes, we are alerted
    about that. The fix for that is to restart ntpsec.service and now it should
    be done through this cookbook.

    Example usage:
        cookbook sre.dns.roll-restart-ntp \
                --alias 'A:dnsbox' \
                --task-id T12345 \
                --reason 'Restarting ntp service' \

        cookbook sre.dns.roll-restart-ntp \
                --alias 'A:dnsbox' \
                --task-id T12345 \
                --reason 'Restarting ntp host' \
                --grace-sleep 900
    """

    # We want to operate in batches of one for a variety of reasons so we
    # should make sure that it is the default.
    batch_default = 1
    batch_max = 1

    # 10 minutes is probably the minimum acceptable time in between the restart
    # of ntpsec.service to establish some NTP sync with the public pools or the
    # other hosts.
    min_grace_sleep = 600
    # The default is 15 minutes, since 10 minutes is somewhat best-case.
    grace_sleep = 900

    valid_actions = ('restart_daemons',)

    def get_runner(self, args) -> SREBatchRunnerBase:
        """As specified by Spicerack API."""
        return NTPRollRestartRunner(args, self.spicerack)


class NTPRollRestartRunner(SREBatchRunnerBase):
    """Rooling reboot of ntpsec.service on DNS hosts."""

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        return ['dnsbox']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['ntpsec']
