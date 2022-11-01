r"""WMCS VPS - Add a user to a project.

Usage example:
    cookbook wmcs.vps.add_user_to_project \
        --cluster-name eqiad1 \
        --project toolsbeta \
        --user dcaro \
        --as-projectadmin

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class AddUserToProject(CookbookBase):
    """WMCS VPS cookbook to add a user to a project."""

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
            help="Username to add to the project",
        )
        parser.add_argument(
            "--as-projectadmin",
            action="store_true",
            default=False,
            help="If set, the user will be added as project admin (otherwise will just add as user)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, AddUserToProjectRunner,)(
            user=args.user,
            cluster_name=args.cluster_name,
            as_projectadmin=args.as_projectadmin,
            spicerack=self.spicerack,
        )


class AddUserToProjectRunner(WMCSCookbookRunnerBase):
    """Runner for AddUserToProject."""

    def __init__(
        self,
        common_opts: CommonOpts,
        user: str,
        as_projectadmin: bool,
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
        self.role_name = "projectadmin" if as_projectadmin else "user"
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        self.openstack_api.role_add(role_name=self.role_name, user_name=self.user)
        self.sallogger.log(f"Added user {self.user} to the project as {self.role_name}")
