"""Rolling service restart or reboot of durum, the Wikimedia DNS check service.

This is based on the roll-restart-reboot-wikimedia-dns cookbook, with relevant
changes to durum.
"""

from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class DurumRestart(SREBatchBase):
    """Rolling restart of durum.

    Example usage:
        cookbook sre.dns.roll-restart-reboot-durum \
            --query 'A:durum-eqiad and not P{durum1001*}' \
            --reason "Scheduled maintenance" \
            reboot

        cookbook sre.dns.roll-restart-reboot-durum \
            --query 'A:durum-eqiad and not P{durum1001*}' \
            --reason "Scheduled maintenance" \
            --task-id "T12345" \
            --ignore-restart-errors \
            --batchsize 2 \
            --grace-sleep 90 \
            restart_daemons
    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args) -> SREBatchRunnerBase:
        """As specified by Spicerack API."""
        return Runner(args, self.spicerack)


class Runner(SREBatchRunnerBase):
    """durum restart Cookbook runner."""

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        aliases = ['durum']
        for datacenter in ALL_DATACENTERS:
            aliases.append(f"durum-{datacenter}")
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # This query must include all hosts matching all the allowed_aliases
        return "A:durum"

    @property
    def restart_daemons(self) -> list:
        """Required by Spicerack API for execution"""
        # These services should automatically stop/start bird.service per
        # systemd dependency ordering.
        return ["nginx.service"]
