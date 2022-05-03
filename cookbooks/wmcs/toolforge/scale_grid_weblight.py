r"""WMCS Toolforge - scale the grid with a new web lighttpd node.

Usage example:
    cookbook wmcs.toolforge.scale_grid_weblight \\
        --project toolsbeta
"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, DebianVersion, OpenstackServerGroupPolicy, add_common_opts, with_common_opts
from cookbooks.wmcs.toolforge.grid.node.lib.create_join_pool import ToolforgeGridNodeCreateJoinPool
from cookbooks.wmcs.vps.create_instance_with_prefix import (
    InstanceCreationOpts,
    add_instance_creation_options,
    with_instance_creation_options,
)

LOGGER = logging.getLogger(__name__)


class ToolforgeScaleGridWeblight(CookbookBase):
    """WMCS Toolforge cookbook to scale the grid with a new weblight node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        add_instance_creation_options(parser)
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
        return with_common_opts(
            self.spicerack,
            args,
            with_instance_creation_options(
                args,
                ToolforgeScaleGridWeblightRunner,
            ),
        )(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            debian_version=DebianVersion[args.debian_version.upper()],
            spicerack=self.spicerack,
        )


class ToolforgeScaleGridWeblightRunner(CookbookRunnerBase):
    """Runner for ToolforgeScaleGridWeblight"""

    def __init__(
        self,
        common_opts: CommonOpts,
        grid_master_fqdn: str,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
        debian_version: DebianVersion = DebianVersion.BUSTER,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.debian_version = debian_version
        self.instance_creation_opts = instance_creation_opts

    def run(self) -> Optional[int]:
        """Main entry point"""
        inner_args = (
            [
                "--security-group",
                "webserver",
                "--server-group",
                f"{self.common_opts.project}-sgegrid-weblight-nodes",
                "--server-group-policy",
                OpenstackServerGroupPolicy.SOFT_ANTI_AFFINITY.value,
                "--debian-version",
                self.debian_version.name.lower(),
                "--nodetype",
                "weblight",
            ]
            + self.common_opts.to_cli_args()
            + self.instance_creation_opts.to_cli_args()
        )

        create_node_cookbook = ToolforgeGridNodeCreateJoinPool(spicerack=self.spicerack)
        create_node_cookbook_arg_parser = create_node_cookbook.argument_parser()
        create_node_cookbook_runner = create_node_cookbook.get_runner(
            create_node_cookbook_arg_parser.parse_args(inner_args)
        )
        create_node_cookbook_runner.run()
