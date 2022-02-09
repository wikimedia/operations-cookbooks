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

from cookbooks.wmcs import CommonOpts, add_common_opts, dologmsg, with_common_opts
from cookbooks.wmcs.toolforge.grid import GridController

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
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--master-node-fqdn",
            required=False,
            default=None,
            help=(
                "Name of the grid master node, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeGridReconfigureRunner,)(
            master_node_fqdn=args.master_node_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            spicerack=self.spicerack,
        )


class ToolforgeGridReconfigureRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridReconfigure"""

    def __init__(
        self,
        common_opts: CommonOpts,
        master_node_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.master_node_fqdn = master_node_fqdn
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(common_opts=self.common_opts, message="reconfiguring the grid by using grid-configurator")

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.master_node_fqdn)
        grid_controller.reconfigure(is_tools_project=(self.common_opts.project == "tools"))
