r"""WMCS VPS - Create a new project

Usage example:
    cookbook wmcs.vps.create_project \
        --cluster-name eqiad1 \
        --project my_fancy_new_project

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class CreateProject(CookbookBase):
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
        # Hack around having the project flag created with add_common_opts
        project_action = next(
            action for action in parser._actions if action.dest == "project"  # pylint: disable=protected-access
        )
        project_action.help = "Name of the project to create."
        project_action.default = None
        project_action.required = True
        parser.add_argument(
            "--description",
            required=True,
            type=str,
            help="Description for the new CloudVps project",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, CreateProjectRunner,)(
            description=args.description,
            cluster_name=args.cluster_name,
            spicerack=self.spicerack,
        )


class CreateProjectRunner(WMCSCookbookRunnerBase):
    """Runner for CreateProject."""

    def __init__(
        self,
        common_opts: CommonOpts,
        description: str,
        cluster_name: OpenstackClusterName,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            cluster_name=cluster_name,
        )
        self.description = description

        self.common_opts = common_opts
        super().__init__(spicerack=spicerack)

    def run(self) -> None:
        """Main entry point"""
        self.openstack_api.project_create(project=self.common_opts.project, description=self.description)
        sallogger = SALLogger(project="admin", task_id=self.common_opts.task_id, dry_run=self.common_opts.no_dologmsg)
        sallogger.log(f"Created project {self.common_opts.project} with default quotas.")
