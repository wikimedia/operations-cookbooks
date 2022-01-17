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

from cookbooks.wmcs import CommonOpts, add_common_opts, dologmsg, with_common_opts
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
        parser.add_argument("--new-node-fqdn", required=True, help="FQDN of the new node.")
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, it will try to add the node even if it's half set up.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(args, ToolforgeGridNodeJoinRunner,)(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            new_node_fqdn=args.new_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodeJoinRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeJoin."""

    def __init__(
        self,
        common_opts: CommonOpts,
        new_node_fqdn: str,
        grid_master_fqdn: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.new_node_fqdn = new_node_fqdn
        self.force = force

    def run(self) -> Optional[int]:
        """Main entry point"""
        if self.new_node_fqdn.find(".") < 0:
            LOGGER.error("ERROR: the --new-node-fqdn argument requires a FQDN")
            return

        dologmsg(
            common_opts=self.common_opts,
            message=f"trying to join node {self.new_node_fqdn} to the grid cluster in {self.common_opts.project}.",
        )

        # a puppet run is required to make sure grid config files are generated
        LOGGER.info(
            "INFO: running puppet before adding node %s to the grid in %s", self.new_node_fqdn, self.common_opts.project
        )
        node = self.spicerack.remote().query(f"D{{{self.new_node_fqdn}}}", use_sudo=True)
        PuppetHosts(remote_hosts=node).run(timeout=30 * 60)

        LOGGER.info("INFO: adding the node %s to the grid in %s", self.new_node_fqdn, self.common_opts.project)
        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        grid_controller.add_node(
            host_fqdn=self.new_node_fqdn, is_tools_project=(self.common_opts.project == "tools"), force=self.force
        )
