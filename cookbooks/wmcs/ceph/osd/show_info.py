r"""WMCS Ceph - Show information about the osds in the cluster

Usage example:
    cookbook wmcs.ceph.osd.show_info \
        --cluster-name eqiad1

"""
import argparse
import logging
from typing import Any, Dict, List

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.inventory import CephClusterName

LOGGER = logging.getLogger(__name__)


class ShowInfo(CookbookBase):
    """WMCS Ceph cookbook to show some information on the osds in the cluster."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--cluster-name",
            required=True,
            choices=list(CephClusterName),
            type=CephClusterName,
            help="Ceph cluster to show information for.",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ShowInfoRunner(
            cluster_name=args.cluster_name,
            spicerack=self.spicerack,
        )


def _print_nodes(nodes_tree: Dict[str, Any]) -> None:
    # we expect a tree with one single root node from ceph
    print("root:")
    for node in sorted(nodes_tree["children"], key=lambda x: x["name"]):
        print(f"  {node['name']}(type:{node['type']})")
        for osd in sorted(node["children"], key=lambda x: x.osd_id):
            print(f"    {osd.name}(class:{osd.device_class}) {osd.status} weight:{osd.crush_weight}")


def _print_stray(stray_nodes: List[Dict[str, Any]]) -> None:
    # TODO: improve once we have an example
    print(f"stray: {stray_nodes}")


class ShowInfoRunner(CookbookRunnerBase):
    """Runner for BootstrapAndAdd"""

    def __init__(
        self,
        cluster_name: CephClusterName,
        spicerack: Spicerack,
    ):
        """Init"""
        self.cluster_controller = CephClusterController(
            remote=spicerack.remote(), cluster_name=cluster_name, spicerack=spicerack
        )

    def run(self) -> None:
        """Main entry point"""
        osd_tree = self.cluster_controller.get_osd_tree()
        _print_nodes(osd_tree.get("nodes", {}))
        _print_stray(osd_tree.get("stray", {}))
