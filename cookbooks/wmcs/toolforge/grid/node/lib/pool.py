r"""WMCS Toolforge - grid - pool an existing grid exec/web node into the cluster

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.pool \\
        --project toolsbeta \\
        --node-hostname toolsbeta-sgewebgen-09-2
"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import (
    OpenstackAPI,
    dologmsg,
    parser_type_str_hostname,
    CommonOpts,
    with_common_opts,
    add_common_opts,
)
from cookbooks.wmcs.toolforge.grid import GridController, GridNodeNotFound

LOGGER = logging.getLogger(__name__)


class ToolforgeGridNodePool(CookbookBase):
    """WMCS Toolforge cookbook to pool a grid node"""

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
        parser.add_argument(
            "--node-hostname", required=True, help="FQDN of the new node.", type=parser_type_str_hostname
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(args, ToolforgeGridNodePoolRunner,)(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            node_hostname=args.node_hostname,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodePoolRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodePool."""

    def __init__(
        self,
        common_opts: CommonOpts,
        node_hostname: str,
        grid_master_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.node_hostname = node_hostname

    def run(self) -> Optional[int]:
        """Main entry point"""
        openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn="cloudcontrol1005.wikimedia.org",
            project=self.common_opts.project,
        )
        if not openstack_api.server_exists(self.node_hostname, print_output=False, print_progress_bars=False):
            LOGGER.warning("node %s is not a VM in project %s", self.node_hostname, self.common_opts.project)
            return 1

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        try:
            grid_controller.pool_node(hostname=self.node_hostname)
        except GridNodeNotFound:
            LOGGER.warning("node %s not found in the %s grid", self.node_fqdn, self.common_opts.project)
            return 1

        dologmsg(common_opts=self.common_opts, message=f"pooled grid node {self.node_hostname}")
        return 0
