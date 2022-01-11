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

from cookbooks.wmcs import OpenstackAPI, GridController, GridNodeNotFound, dologmsg

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
        parser.add_argument(
            "--project",
            required=True,
            help="Openstack project where the toolforge installation resides.",
        )
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
        parser.add_argument("--node-fqdn", required=True, help="FQDN of the new node.")
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeGridNodeDepoolRunner(
            project=args.project,
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            node_fqdn=args.node_fqdn,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodeDepoolRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeDepool."""

    def __init__(
        self,
        project: str,
        node_fqdn: str,
        grid_master_fqdn: str,
        task_id: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.project = project
        self.grid_master_fqdn = grid_master_fqdn
        self.task_id = task_id
        self.spicerack = spicerack
        self.node_fqdn = node_fqdn

    def run(self) -> Optional[int]:
        """Main entry point"""
        openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn="cloudcontrol1005.wikimedia.org",
            project=self.project
        )
        if not openstack_api.server_exists(self.node_fqdn, print_output=False):
            LOGGER.warning("node %s is not a VM in project %s", self.node_fqdn, self.project)
            return 1

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        try:
            grid_controller.depool_node(host_fqdn=self.node_fqdn)
        except GridNodeNotFound:
            LOGGER.warning("node %s not found in the %s grid", self.node_fqdn, self.project)
            return 1

        dologmsg(
            project=self.project,
            message=f"depooled grid node {self.node_fqdn}",
            task_id=self.task_id,
        )
        return 0
