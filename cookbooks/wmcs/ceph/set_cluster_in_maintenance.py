"""WMCS Ceph - Set cluster in maintenance.

Usage example:
    cookbook wmcs.ceph.set_cluster_in_maintenance \
        --monitor-node-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.lib.ceph import CephClusterController

LOGGER = logging.getLogger(__name__)


class SetClusterInMaintenance(CookbookBase):
    """WMCS Ceph cookbook to set a cluster in maintenance."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--monitor-node-fqdn",
            required=True,
            help="FQDN of one of the monitor nodes to manage the cluster.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return SetClusterInMaintenanceRunner(
            monitor_node_fqdn=args.monitor_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class SetClusterInMaintenanceRunner(CookbookRunnerBase):
    """Runner for SetClusterInMaintenance"""

    def __init__(
        self,
        monitor_node_fqdn: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.monitor_node_fqdn = monitor_node_fqdn
        self.force = force
        self.spicerack = spicerack

    def run(self) -> None:
        """Main entry point"""
        controller = CephClusterController(remote=self.spicerack.remote(), controlling_node_fqdn=self.monitor_node_fqdn)
        controller.set_maintenance(force=self.force)
