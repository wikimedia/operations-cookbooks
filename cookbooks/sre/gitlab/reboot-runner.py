"""GitLab Runner reboot cookbook"""

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class RebootRunner(SREBatchBase):
    """Gracefully reboot a GitLab Runner host

    - stop gitlab-runner daemon gracefully
    - Set Icinga/Alertmanager downtime for all hosts in the batch to reboot
    - Reboot
    - Wait for hosts to come back online
    - If reboot: Wait for the first puppet run
    - Wait for Icinga optimal status

    Graceful reboot means runnig CI jobs will get executed but no new
    jobs will be scheduled

    Usage example:
        cookbook sre.gitlab.reboot-runner

    """

    batch_default = 1
    valid_actions = ('reboot',)

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootRunnerRunner(args, self.spicerack)


class RebootRunnerRunner(SREBatchRunnerBase):
    """Gracefully reboot a GitLab Runner host runner."""

    @property
    def allowed_aliases(self):
        """Required by RebootRunnerBase"""
        return ['gitlab-runner']

    @property
    def pre_scripts(self):
        """Stops gitlab-runner process gracefully by sending SIGQUIT"""
        return ['/usr/bin/systemctl kill -s 3 gitlab-runner.service']
