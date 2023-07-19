"""Rolling service restart or reboot of durum, the Wikimedia DNS check service.

This is based on the roll-restart-reboot-wikimedia-dns cookbook, with relevant
changes to durum.
"""

from spicerack.remote import RemoteHosts
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

    disable_puppet_on_restart = True
    disable_puppet_on_reboot = True

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        aliases = []
        for datacenter in ALL_DATACENTERS:
            aliases.append(f"durum-{datacenter}")
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # This query must include all hosts matching all the allowed_aliases
        return "A:durum"

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Run before performing the action on the batch of hosts."""
        hosts.run_async("/bin/systemctl stop bird.service")

    def post_action(self, hosts: RemoteHosts) -> None:
        """Run after performing the action on the batch of hosts."""
        hosts.run_async("/bin/systemctl start bird.service")

    @property
    def restart_daemons(self) -> list:
        """Required by Spicerack API for execution"""
        # These services should automatically stop/start bird.service per
        # systemd dependency ordering.
        return ["nginx.service"]
