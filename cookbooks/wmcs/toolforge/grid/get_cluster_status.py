"""WMCS Toolforge - grid - get cluster status

Usage example:
    cookbook wmcs.toolforge.grid.get_cluster_status \
        --project toolsbeta \
        --master-node-fqdn toolsbeta-test-etcd-8.toolsbeta.eqiad1.wikimedia.cloud
"""
import argparse
import logging
from typing import Optional
import yaml

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.toolforge.grid import GridController

LOGGER = logging.getLogger(__name__)


class NoAliasDumper(yaml.Dumper):  # pylint: disable=too-many-ancestors
    """Class override for the yaml module."""

    def ignore_aliases(self, data):
        """Function override, resolve yaml references."""
        return True


class ToolforgeGridGetClusterStatus(CookbookBase):
    """Toolforge cookbook to get the current grid cluster status"""

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

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeGridGetClusterStatusRunner(
            master_node_fqdn=args.master_node_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            project=args.project,
            spicerack=self.spicerack,
        )


class ToolforgeGridGetClusterStatusRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridGetClusterStatus"""

    def __init__(
        self,
        master_node_fqdn: str,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.master_node_fqdn = master_node_fqdn
        self.project = project
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.master_node_fqdn)
        print(yaml.dump(grid_controller.get_nodes_info(), Dumper=NoAliasDumper))
