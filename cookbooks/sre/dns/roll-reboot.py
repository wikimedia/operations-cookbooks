"""Rolling reboot of DNS hosts identified by the cumin alias A:dnsbox."""

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class DNSBoxRollReboot(SREBatchBase):
    """Rolling reboot of DNS hosts.

    This cookbook is for the rolling reboots of the DNS hosts, referred to by
    the cumin alias A:dnsbox. This covers both the DNS rec and auth hosts since
    a given DNS box serves both roles.

    Example usage:
        cookbook sre.dns.roll-reboot \
                --alias 'A:dnsbox' \
                --task-id T12345 \
                --reason 'Restarting DNS host' \

        cookbook sre.dns.roll-reboot \
                --alias 'A:dnsbox' \
                --task-id T12345 \
                --reason 'Restarting DNS host' \
                --grace-sleep 900
    """

    # We want to operate in batches of one for a variety of reasons so we
    # should make sure that it is the default.
    batch_default = 1
    batch_max = 1

    # 10 minutes is probably the minimum acceptable time in between the reboot
    # of each host to establish some NTP sync with the public pools or the
    # other hosts.
    min_grace_sleep = 600
    # The default is 15 minutes, since 10 minutes is somewhat best-case.
    grace_sleep = 900

    valid_actions = ('reboot',)

    def get_runner(self, args) -> SRELBBatchRunnerBase:
        """As specified by Spicerack API."""
        return DNSBoxRebootRunner(args, self.spicerack)


class DNSBoxRebootRunner(SRELBBatchRunnerBase):
    """Rooling reboot of DNS hosts."""

    # Let's wait a bit after depooling and re-pooling to make sure changes are
    # pulled in by confd on the respective host.
    deepol_sleep = 60
    repool_sleep = 60

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        return ['dnsbox', 'dns-rec', 'dns-auth']

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # This query must include all hosts matching all the allowed_aliases
        return 'A:dnsbox'
