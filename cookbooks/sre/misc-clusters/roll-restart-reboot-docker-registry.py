"""Docker registry roll operations cookbook."""
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class DockerRegistryRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart the Docker registry

    Usage example:
        cookbook sre.misc-clusters.roll-restart-reboot-docker-registry \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.misc-clusters.roll-restart-reboot-docker-registry \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    owner_team = 'ServiceOps'
    batch_default = 1
    grace_sleep = 2

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return DockerRegistryRestartRebootRunner(args, self.spicerack)


class DockerRegistryRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart the Docker registry cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['docker-registry']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['nginx', 'docker-registry']
