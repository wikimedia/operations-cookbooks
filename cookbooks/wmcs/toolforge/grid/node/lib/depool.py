r"""WMCS Toolforge - grid - depool an existing grid exec/web node from the cluster

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.depool \\
        --project toolsbeta \\
        --node-fqdn toolsbeta-sgewebgen-09-2.toolsbeta.eqiad1.wikimedia.cloud
"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, OpenstackAPI, add_common_opts, dologmsg, with_common_opts
from cookbooks.wmcs.toolforge.grid import GridController, GridNodeNotFound

LOGGER = logging.getLogger(__name__)


class ToolforgeGridNodeDepool(CookbookBase):
    """WMCS Toolforge cookbook to depool a grid node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--grid-master-fqdn",
            required=False,
            default=None,
            help=(
                "FQDN of the grid master, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        parser.add_argument("--node-fqdn", required=True, help="FQDN of the new node.")
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(args, ToolforgeGridNodeDepoolRunner,)(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            node_fqdn=args.node_fqdn,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodeDepoolRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeDepool."""

    def __init__(
        self,
        common_opts: CommonOpts,
        node_fqdn: str,
        grid_master_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.node_fqdn = node_fqdn

    def run(self) -> Optional[int]:
        """Main entry point"""
        openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn="cloudcontrol1005.wikimedia.org",
            project=self.common_opts.project,
        )
        if not openstack_api.server_exists(self.node_fqdn, print_output=False):
            LOGGER.warning("node %s is not a VM in project %s", self.node_fqdn, self.common_opts.project)
            return 1

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        try:
            grid_controller.depool_node(host_fqdn=self.node_fqdn)
        except GridNodeNotFound:
            LOGGER.warning("node %s not found in the %s grid", self.node_fqdn, self.common_opts.project)
            return 1

        dologmsg(
            common_opts=self.common_opts,
            message=f"depooled grid node {self.node_fqdn}",
        )

        return 0
