"""Disable (and re-enable) Puppet merges during maintainance or incidents ."""

from argparse import Namespace
from wmflib.interactive import confirm_on_failure
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase



class DisablePuppet(CookbookBase):
    """Disable Puppet merges on all puppet servers

    This runs a command to disable Puppet merges on all Puppet servers (to avoid
    surprises if the Puppet merge server needs to be failed over as part of an
    incident.

    Usage example:
        cookbook sre.puppet.disable-merges 'Everything is on fire'
        cookbook sre.puppet.disable-merges --reenable

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("message", help="Message why Puppet merges are disabled")
        parser.add_argument("--reenable",
                            help="Use this to allow Puppet merges again",
                            action="store_true")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return DisablePuppetRunner(args, self.spicerack)


class DisablePuppetRunner(CookbookRunnerBase):
    """Disable Puppet merges on all puppet servers."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Init function.

        Arguments:
            args (Namespace): the parse arguments
            spicerack (Spicerack): An initiated spicerack object

        """
        self.args = args
        self.puppetservers = spicerack.remote().query("A:puppetserver")
        self.reason = spicerack.admin_reason(args.message, task_id=args.task_id)

    def run(self) -> int:
        """Generate data"""

        if self.args.reenable:
            command = 'sudo rm -f /var/lock/puppet-merge-lockout-tagout'
            confirm_on_failure(self.puppetservers.run_sync, command)
            return 0

        command = f'sudo /usr/local/bin/puppet-merge --lockout-tagout "{self.args.message}"'
        confirm_on_failure(self.puppetservers.run_sync, command)
        return 0
