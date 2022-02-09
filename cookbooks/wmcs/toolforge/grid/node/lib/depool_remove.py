r"""WMCS Toolforge - grid - depool and remove an existing grid exec/web node from the cluster

NOTE: also deletes the virtual machine!

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.depool_remove \\
        --project toolsbeta \\
        --node-hostname toolsbeta-sgewebgen-09-2
"""
import argparse
import logging
import time
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import (
    CommonOpts,
    OpenstackAPI,
    add_common_opts,
    dologmsg,
    with_common_opts,
    parser_type_str_hostname,
)
from cookbooks.wmcs.toolforge.grid import GridController, GridNodeNotFound
from cookbooks.wmcs.vps.remove_instance import RemoveInstance

LOGGER = logging.getLogger(__name__)


class ToolforgeGridNodeDepoolRemove(CookbookBase):
    """WMCS Toolforge cookbook to remove any grid node from the cluster"""

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
            "--node-hostname", required=True, help="hostname of the node to delete.", type=parser_type_str_hostname
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeGridNodeDepoolRemoveRunner,)(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            node_hostname=args.node_hostname,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodeDepoolRemoveRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeDepoolRemove."""

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
        if not openstack_api.server_exists(self.node_hostname, print_output=False):
            LOGGER.warning("%s is not an openstack VM in project %s", self.node_hostname, self.common_opts.project)
            return

        # before we start, notify folks
        dologmsg(
            common_opts=self.common_opts,
            message=f"removing grid node {self.node_hostname} (depool/drain, remove VM and reconfigure grid)",
        )

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)

        # step 1
        LOGGER.info("STEP 1: depool/drain grid node %s", self.node_hostname)
        try:
            grid_controller.depool_node(host_fqdn=self.node_hostname)
            LOGGER.info(
                "depooled/drained node %s, now waiting a couple minutes so jobs are rescheduled", self.node_hostname
            )
            time.sleep(60 * 2)
        except GridNodeNotFound:
            LOGGER.info(
                "can't depool node %s, not found in the %s grid, continuing with other steps anyway",
                self.node_hostname,
                self.common_opts.project,
            )

        # step 2
        LOGGER.info("STEP 2: delete the virtual machine %s", self.node_hostname)
        remove_instance_cookbook = RemoveInstance(self.spicerack)
        remove_args = [
            "--server-name",
            self.node_hostname,
            "--no-dologmsg",  # not interested in SAL logs for the internal call
        ] + self.common_opts.to_cli_args()

        remove_instance_cookbook_runner = remove_instance_cookbook.get_runner(
            args=remove_instance_cookbook.argument_parser().parse_args(remove_args)
        )
        remove_instance_cookbook_runner.run()
        LOGGER.info("removed VM %s, now waiting a minute so openstack can actually remove it", self.node_hostname)
        time.sleep(60 * 1)

        # step 3
        LOGGER.info("STEP 3: reconfigure the grid so it knows %s no longer exists", self.node_hostname)
        grid_controller.reconfigure(is_tools_project=(self.common_opts.project == "tools"))

        # all done
        LOGGER.info("all operations done. Congratulations, you have one less grid node.")
