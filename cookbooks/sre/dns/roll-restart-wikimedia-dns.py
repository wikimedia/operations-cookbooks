"""Rolling restart of all services comprising Wikimedia DNS."""

from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class WDNSRestart(SREBatchBase):
    """Rolling restart of Wikimedia DNS services

    Example usage:
        cookbook sre.cdn.roll-restart-wikimedia-dns \
            --alias doh-codfw \
            --reason "Scheduled maintenance"

        cookbook sre.cdn.roll-restart-wikimedia-dns \
            --query 'A:doh-eqiad and not P{doh1001*}' \
            --reason "Scheduled maintenance" \
            --task-id "T12345" \
            --ignore-restart-errors \
            --batchsize 2 \
            --grace-sleep 90
    """

    batch_default = 1
    grace_sleep = 30
    valid_actions = ('restart_daemons',)

    def get_runner(self, args) -> SREBatchRunnerBase:
        """As specified by Spicerack API."""
        return Runner(args, self.spicerack)


class Runner(SREBatchRunnerBase):
    """Wikimedia DNS restart Cookbook runner."""

    disable_puppet_on_restart = True

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        aliases = []
        for datacenter in ALL_DATACENTERS:
            aliases.append(f"wikidough-{datacenter}")
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # This query must include all hosts matching all the allowed_aliases
        return "A:wikidough"

    @property
    def restart_daemons(self) -> list:
        """Required by Spicerack API for execution"""
        # These services should automatically stop/start bird.service per
        # systemd dependency ordering.
        return ["pdns-recursor.service", "dnsdist.service"]
