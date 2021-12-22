"""WMCS Toolforge - grid - reconfigure

Usage example:
    cookbook wmcs.toolforge.grid.reconfigure \
        --project toolsbeta \
        --master-node-fqdn toolsbeta-sgegrid-master.toolsbeta.eqiad1.wikimedia.cloud \
        --no-dologmsg
"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import GridController, dologmsg

LOGGER = logging.getLogger(__name__)


class ToolforgeGridReconfigure(CookbookBase):
    """Toolforge cookbook to reconfigure the grid"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--master-node-fqdn",
            required=False,
            default=None,
            help=(
                "Name of the grid master node, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this operation (ex. T123456)",
        )
        parser.add_argument(
            "--no-dologmsg",
            required=False,
            action='store_true',
            help="To disable dologmsg calls",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeGridReconfigureRunner(
            master_node_fqdn=args.master_node_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            project=args.project,
            spicerack=self.spicerack,
            no_dologmsg=args.no_dologmsg,
            task_id=args.task_id,
        )


class ToolforgeGridReconfigureRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridReconfigure"""

    def __init__(
        self,
        master_node_fqdn: str,
        project: str,
        spicerack: Spicerack,
        no_dologmsg: bool = False,
        task_id: Optional[str] = None,
    ):
        """Init"""
        self.master_node_fqdn = master_node_fqdn
        self.project = project
        self.spicerack = spicerack
        self.no_dologmsg = no_dologmsg
        self.task_id = task_id

    def run(self) -> Optional[int]:
        """Main entry point"""
        if not self.no_dologmsg:
            dologmsg(
                project=self.project,
                message="reconfiguring the grid by using grid-configurator",
                task_id=self.task_id,
            )

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.master_node_fqdn)
        grid_controller.reconfigure(is_tools_project=(self.project == "tools"))
