"""WMCS Ceph - Unset cluster maintenance.

Usage example:
    cookbook wmcs.ceph.unset_cluster_maintenance \
        --monitor-node-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CephClusterController

LOGGER = logging.getLogger(__name__)


class UnSetClusterInMaintenance(CookbookBase):
    """WMCS Ceph cookbook to unset a cluster maintenance."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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
        return UnSetClusterInMaintenanceRunner(
            monitor_node_fqdn=args.monitor_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class UnSetClusterInMaintenanceRunner(CookbookRunnerBase):
    """Runner for UnSetClusterInMaintenance"""

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

    def run(self) -> Optional[int]:
        """Main entry point"""
        controller = CephClusterController(remote=self.spicerack.remote(), controlling_node_fqdn=self.monitor_node_fqdn)
        controller.unset_maintenance(force=self.force)
