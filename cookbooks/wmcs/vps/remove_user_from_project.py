r"""WMCS VPS - Remove a user from a project.

Usage example:
    cookbook wmcs.vps.remove_user_from_project \
        --cluster-name eqiad1 \
        --project toolsbeta \
        --user dcaro

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class RemoveUserFromProject(CookbookBase):
    """WMCS VPS cookbook to remove a user from a project."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
        parser.add_argument(
            "--cluster-name",
            required=False,
            choices=list(OpenstackClusterName),
            default=OpenstackClusterName.EQIAD1,
            type=OpenstackClusterName,
            help="Openstack cluster name to use.",
        )
        parser.add_argument(
            "--user",
            help="Username to remove from the project",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RemoveUserFromProjectRunner,)(
            user=args.user,
            cluster_name=args.cluster_name,
            spicerack=self.spicerack,
        )


class RemoveUserFromProjectRunner(WMCSCookbookRunnerBase):
    """Runner for RemoveUserFromProject."""

    def __init__(
        self,
        common_opts: CommonOpts,
        user: str,
        cluster_name: OpenstackClusterName,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            cluster_name=cluster_name,
            project=self.common_opts.project,
        )

        self.user = user
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        for role in self.openstack_api.role_list_assignments(user_name=self.user):
            self.openstack_api.role_remove(role=role["Role"], user_name=self.user)
        self.sallogger.log(f"Removed user {self.user} from the project")
