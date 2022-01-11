r"""WMCS Toolforge - scale the grid with a new grid exec node.

Usage example:
    cookbook wmcs.toolforge.scale_grid_exec \\
        --project toolsbeta
"""
# pylint: disable=too-many-arguments
import argparse
import logging
from typing import Optional
from enum import Enum

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackServerGroupPolicy
from cookbooks.wmcs.toolforge.grid.node.lib.create_join_pool import ToolforgeGridNodeCreateJoinPool
from cookbooks.wmcs.toolforge.start_instance_with_prefix import (
    InstanceCreationOpts,
    add_instance_creation_options,
    with_instance_creation_options,
)

LOGGER = logging.getLogger(__name__)


class DebianVersion(Enum):
    """Represents Debian release names/numbers."""

    STRETCH = "09"
    BUSTER = "10"


class ToolforgeScaleGridExec(CookbookBase):
    """WMCS Toolforge cookbook to scale the grid with a new exec node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "--project",
            required=True,
            help="Openstack project where the toolforge installation resides.",
        )
        add_instance_creation_options(parser)
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this operation (ex. T123456)",
        )
        parser.add_argument(
            "--grid-master-fqdn",
            required=False,
            default=None,
            help=(
                "FQDN of the grid master, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        parser.add_argument(
            "--debian-version",
            required=True,
            default=DebianVersion.BUSTER.name.lower(),
            choices=[version.name.lower() for version in DebianVersion],
            # TODO: Figure out the debian version from the image, or just not use it for the prefix
            help="Version of debian to use, as currently we are unable to get it from the image reliably.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_instance_creation_options(args, ToolforgeScaleGridExecRunner,)(
            project=args.project,
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            debian_version=DebianVersion[args.debian_version.upper()],
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class ToolforgeScaleGridExecRunner(CookbookRunnerBase):
    """Runner for ToolforgeScaleGridExec"""

    def __init__(
        self,
        project: str,
        grid_master_fqdn: str,
        task_id: str,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
        debian_version: DebianVersion = DebianVersion.BUSTER,
    ):
        """Init"""
        self.project = project
        self.grid_master_fqdn = grid_master_fqdn
        self.task_id = task_id
        self.spicerack = spicerack
        self.debian_version = debian_version
        self.instance_creation_opts = instance_creation_opts

    def run(self) -> Optional[int]:
        """Main entry point"""
        inner_args = [
            "--project",
            self.project,
            "--security-group",
            "execnode",
            "--server-group",
            f"{self.project}-sgegrid-exec-nodes",
            "--server-group-policy",
            OpenstackServerGroupPolicy.SOFT_ANTI_AFFINITY.value,
            "--debian-version",
            self.debian_version.name.lower(),
            "--nodetype",
            "exec",
        ] + self.instance_creation_opts.to_cli_args()

        create_node_cookbook = ToolforgeGridNodeCreateJoinPool(spicerack=self.spicerack)
        create_node_cookbook_arg_parser = create_node_cookbook.argument_parser()
        create_node_cookbook_runner = create_node_cookbook.get_runner(
            create_node_cookbook_arg_parser.parse_args(inner_args)
        )
        create_node_cookbook_runner.run()
