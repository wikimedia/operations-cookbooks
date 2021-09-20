"""WMCS Ceph - Upgrade all the mon nodes.

Usage example:
    cookbook wmcs.ceph.upgrade_mons \
        --controlling-node-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CephClusterController
from cookbooks.wmcs.ceph.upgrade_ceph_node import UpgradeCephNode

LOGGER = logging.getLogger(__name__)


class UpgradeMons(CookbookBase):
    """WMCS Ceph cookbook to set a cluster in maintenance."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--controlling-node-fqdn",
            required=True,
            help="FQDN of one of the nodes to manage the cluster.",
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
        return UpgradeMonsRunner(
            controlling_node_fqdn=args.controlling_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class UpgradeMonsRunner(CookbookRunnerBase):
    """Runner for UpgradeMons"""

    def __init__(
        self,
        controlling_node_fqdn: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.controlling_node_fqdn = controlling_node_fqdn
        self.force = force
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.controlling_node_fqdn
        )
        controller.set_maintenance()

        upgrade_ceph_node_cookbook = UpgradeCephNode(spicerack=self.spicerack)
        monitor_nodes = list(controller.get_nodes()["mon"].keys())
        LOGGER.info("Upgrading and rebooting the nodes %s", str(monitor_nodes))

        for index, monitor_node in enumerate(monitor_nodes):
            LOGGER.info("Upgrading node %s, %d done, %d to go", monitor_node, index, len(monitor_nodes) - index)
            args = [
                "--to-upgrade-fqdn",
                f"{monitor_node}.{controller.get_nodes_domain()}",
                "--skip-maintenance",
            ]
            if self.force:
                args.append("--force")

            upgrade_ceph_node_cookbook.get_runner(
                args=upgrade_ceph_node_cookbook.argument_parser().parse_args(args)
            ).run()
            LOGGER.info(
                "Upgraded node %s, %d done, %d to go, waiting for cluster to stabilize...",
                monitor_node,
                index + 1,
                len(monitor_nodes) - index - 1,
            )
            controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        controller.unset_maintenance()
