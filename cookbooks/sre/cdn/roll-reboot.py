"""Depool, unmonitor, and reboot instances one-by-one."""

from wmflib.constants import ALL_DATACENTERS

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class Batch(SREBatchBase):
    """Reboot CP nodes in the CDN

    Example usage:
        cookbook sre.cdn.roll-reboot \
            --alias 'cp-text_ulsfo' \
            --reason 'Kernel update' \
            --task-id T123456

        cookbook sre.cdn.roll-reboot \
            --alias 'cp-text_ulsfo' \
            --reason 'Kernel update' \
            --task-id T123456 \
            --grace-sleep 1200
    """

    batch_default = 1
    min_grace_sleep = 300
    grace_sleep = 1800
    valid_actions = ('reboot',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return Runner(args, self.spicerack)


class Runner(SRELBBatchRunnerBase):
    """Roll reboot/restart a CDN cluster"""

    depool_sleep = 60

    def post_action(self, hosts) -> None:
        """Run this function after performing the action on the batch of hosts.

        We want to keep track of the progress of this cookbook, so we log to
        SAL once a host is rebooted.
        """
        self._spicerack.sal_logger.info('%s finished rebooting %s', __name__, hosts.hosts)
        super().post_action(hosts)

    @property
    def allowed_aliases(self) -> list:
        """Required by SREBatchRunnerBase"""
        aliases = []
        for datacenter in ALL_DATACENTERS:
            aliases.append(f"cp-{datacenter}")
            aliases.append(f"cp-text_{datacenter}")
            aliases.append(f"cp-upload_{datacenter}")
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases
