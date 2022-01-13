r"""WMCS Toolforge - grid - join existing grid exec/web node to the cluster

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.join \\
        --project toolsbeta \\
        --new-node-fqdn toolsbeta-sgewebgen-09-2.toolsbeta.eqiad1.wikimedia.cloud
"""
# pylint: disable=too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs import dologmsg
from cookbooks.wmcs.toolforge.grid import GridController

LOGGER = logging.getLogger(__name__)


class ToolforgeGridNodeJoin(CookbookBase):
    """WMCS Toolforge cookbook to add a new webgrid generic node"""

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
        parser.add_argument("--new-node-fqdn", required=True, help="FQDN of the new node.")
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, it will try to add the node even if it's half set up.",
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
        return ToolforgeGridNodeJoinRunner(
            project=args.project,
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            new_node_fqdn=args.new_node_fqdn,
            task_id=args.task_id,
            force=args.force,
            spicerack=self.spicerack,
            no_dologmsg=args.no_dologmsg,
        )


class ToolforgeGridNodeJoinRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeJoin."""

    def __init__(
        self,
        project: str,
        new_node_fqdn: str,
        grid_master_fqdn: str,
        task_id: str,
        force: bool,
        spicerack: Spicerack,
        no_dologmsg: bool = False,
    ):
        """Init"""
        self.project = project
        self.grid_master_fqdn = grid_master_fqdn
        self.task_id = task_id
        self.spicerack = spicerack
        self.new_node_fqdn = new_node_fqdn
        self.force = force
        self.no_dologmsg = no_dologmsg

    def run(self) -> Optional[int]:
        """Main entry point"""
        if self.new_node_fqdn.find(".") < 0:
            LOGGER.error("ERROR: the --new-node-fqdn argument requires a FQDN")
            return

        if not self.no_dologmsg:
            dologmsg(
                project=self.project,
                message=f"trying to join node {self.new_node_fqdn} to the grid cluster in {self.project}.",
                task_id=self.task_id,
            )

        # a puppet run is required to make sure grid config files are generated
        LOGGER.info("INFO: running puppet before adding node %s to the grid in %s", self.new_node_fqdn, self.project)
        node = self.spicerack.remote().query(f"D{{{self.new_node_fqdn}}}", use_sudo=True)
        PuppetHosts(remote_hosts=node).run(timeout=30 * 60)

        LOGGER.info("INFO: adding the node %s to the grid in %s", self.new_node_fqdn, self.project)
        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        grid_controller.add_node(
            host_fqdn=self.new_node_fqdn, is_tools_project=(self.project == "tools"), force=self.force
        )
