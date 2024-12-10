"""Event schemas roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class EventSchemasRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of EventSchemas

    Usage example:
        cookbook sre.misc-clusters.roll-restart-reboot-eventschemas \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.misc-clusters.roll-restart-reboot-eventschemas \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    owner_team = 'Data Platform'
    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return EventSchemasRestartRebootRunner(args, self.spicerack)


class EventSchemasRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an EventSchemas cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['schema', 'schema-eqiad', 'schema-codfw']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['nginx', 'envoyproxy']
