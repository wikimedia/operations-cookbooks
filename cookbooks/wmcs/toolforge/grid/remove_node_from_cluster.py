r"""WMCS Toolforge - grid - Remove an existing grid exec/web node from the cluster

Note: also deletes the virtual machine!

Usage example:
    cookbook wmcs.toolforge.grid.remove_node_from_cluster \\
        --project toolsbeta \\
        --node-fqdn toolsbeta-sgewebgen-09-2.toolsbeta.eqiad1.wikimedia.cloud
"""
import argparse
import time
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackAPI, GridController, GridNodeNotFound, dologmsg
from cookbooks.wmcs.vps.remove_instance import RemoveInstance

LOGGER = logging.getLogger(__name__)


class ToolforgeGridRemoveNodeFromCluster(CookbookBase):
    """WMCS Toolforge cookbook to remove any grid node from the cluster"""

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
        parser.add_argument("--node-fqdn", required=True, help="FQDN of the node to delete.")
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeGridRemoveNodeFromClusterRunner(
            project=args.project,
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            node_fqdn=args.node_fqdn,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class ToolforgeGridRemoveNodeFromClusterRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridRemoveNodeFromCluster."""

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
            project=self.project,
        )
        if not openstack_api.server_exists(self.node_fqdn, print_output=False):
            LOGGER.warning("%s is not an openstack VM in project %s", self.node_fqdn, self.project)
            return

        # before we start, notify folks
        dologmsg(
            project=self.project,
            message=f"removing grid node {self.node_fqdn} (depool/drain, remove VM and reconfigure grid)",
            task_id=self.task_id,
        )

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)

        # step 1
        LOGGER.info("STEP 1: depool/drain grid node %s", self.node_fqdn)
        try:
            grid_controller.depool_node(host_fqdn=self.node_fqdn)
            LOGGER.info(
                "depooled/drained node %s, now waiting a couple minutes so jobs are rescheduled", self.node_fqdn
            )
            time.sleep(60 * 2)
        except GridNodeNotFound:
            LOGGER.info(
                "can't depool node %s, not found in the %s grid, continuing with other steps anyway",
                self.node_fqdn,
                self.project,
            )

        # step 2
        LOGGER.info("STEP 2: delete the virtual machine %s", self.node_fqdn)
        remove_instance_cookbook = RemoveInstance(self.spicerack)
        remove_args = [
            "--project",
            self.project,
            "--server-name",
            self.node_fqdn.split(".")[0],
            "--dologmsg",
            False,
        ]
        remove_instance_cookbook_runner = remove_instance_cookbook.get_runner(
            args=remove_instance_cookbook.argument_parser().parse_args(remove_args)
        )
        remove_instance_cookbook_runner.run()
        LOGGER.info("removed VM %s, now waiting a minute so openstack can actually remove it", self.node_fqdn)
        time.sleep(60 * 1)

        # step 3
        LOGGER.info("STEP 3: reconfigure the grid so it knows %s no longer exists", self.node_fqdn)
        grid_controller.reconfigure(is_tools_project=(self.project == "tools"))

        # all done
        LOGGER.info("all operations done. Congratulations, you have one less grid node.")
