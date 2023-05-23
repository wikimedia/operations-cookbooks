"""Cookbook to update netbox-extras."""

from argparse import Namespace

from spicerack import Spicerack
from spicerack.remote import RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class UpdateExtrasBase(SREBatchBase):
    """Base class for updating the netbox-extras repository."""

    valid_actions: tuple[str] = ('restart_daemons',)

    def get_runner(self, args):
        """Required by base class."""
        return UpdateExtrasRunner(args, self.spicerack)


class UpdateExtrasRunner(SREBatchRunnerBase):
    """A restart runner to update the netbox-extra repository and restart netbox-uwsgi if neccesary."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Override base to add new param."""
        super().__init__(args, spicerack)
        # We only set this if validation files have changed
        self.needs_restart = False

    @property
    def allowed_aliases(self):
        """Required by base class."""
        return ['netbox', 'netbox-canary']

    @property
    def allowed_aliases_query(self):
        """Return a slightly optimised query then the base class."""
        return "A:netbox-all"

    def action(self, hosts: RemoteHosts) -> None:
        """Action is used to only call the restart action if we need to."""
        if self.needs_restart:
            super().action(hosts)

    @property
    def restart_daemons(self):
        """If we need to reboot return the list of daemons other wise return and empty list."""
        return ['uwsgi-netbox']

    @property
    def pre_scripts(self):
        """Update the extras repository."""
        return ['git -C /srv/deployment/netbox-extras pull --ff-only']

    def pre_action(self, hosts: RemoteHosts):
        """Check to see what has changed before updating.

        We only need to restart if the files changed are contained in the validators directory.
        So we first ask for a diff and check what will change, setting needs_restart appropriately

        Arguments:
            hosts: the hosts to act on

        """
        hosts.run_async("git -C /srv/deployment/netbox-extras fetch origin")
        results = hosts.run_async(
            "git -C /srv/deployment/netbox-extras diff --name-only origin/master"
        )
        for _, output in results:
            for line in output.message().decode().splitlines():
                if line.lstrip().startswith('validators'):
                    self.needs_restart = True
        self._run_scripts(self.pre_scripts, hosts)
