"""Rolling restart of Wikimedia DNS services or full reboot.

Whether restarting the services or rebooting the entire host, typical
decommissioning logic is preserved. systemd unit ordering should safeguard
these units as well.
"""

from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class WDNSRestart(SREBatchBase):
    """Rolling restart of Wikimedia DNS services

    Example usage:
        cookbook sre.dns.roll-restart-reboot-wikimedia-dns \
            --alias wikidough-codfw \
            --reason "Scheduled maintenance" \
            restart_daemons

        cookbook sre.dns.roll-restart-reboot-wikimedia-dns \
            --query 'A:wikidough-eqiad and not P{doh1001*}' \
            --reason "Scheduled maintenance" \
            --task-id "T12345" \
            --ignore-restart-errors \
            --batchsize 2 \
            --grace-sleep 90 \
            restart_daemons

        cookbook sre.dns.roll-restart-reboot-wikimedia-dns \
            --query 'A:wikidough-eqiad and not P{doh1001*}' \
            --reason "Scheduled maintenance" \
            --task-id "T12345" \
            --batchsize 2 \
            reboot
    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args) -> SREBatchRunnerBase:
        """As specified by Spicerack API."""
        return Runner(args, self.spicerack)


class Runner(SREBatchRunnerBase):
    """Wikimedia DNS restart Cookbook runner."""

    def _reboot_action(self, hosts: RemoteHosts, reason: Reason) -> None:
        # Depool by stopping bird, explicitly.
        hosts.run_async("/bin/systemctl stop bird.service")
        super()._reboot_action(hosts, reason)

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
