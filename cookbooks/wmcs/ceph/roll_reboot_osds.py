"""WMCS Ceph - Rolling reboot of all the osd nodes.

Usage example:
    cookbook wmcs.ceph.roll_reboot_osds \
        --controlling-node-fqdn cloudcephmon2001-dev.codfw.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CephController, dologmsg
from cookbooks.wmcs.ceph.reboot_node import RebootNode

LOGGER = logging.getLogger(__name__)


class RollRebootOsds(CookbookBase):
    """WMCS Ceph cookbook to rolling reboot all osds."""

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
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this reboot (ex. T123456)",
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
        return RollRebootOsdsRunner(
            controlling_node_fqdn=args.controlling_node_fqdn,
            task_id=args.task_id,
            force=args.force,
            spicerack=self.spicerack,
        )


class RollRebootOsdsRunner(CookbookRunnerBase):
    """Runner for RollRebootOsds"""

    def __init__(
        self,
        controlling_node_fqdn: str,
        task_id: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.controlling_node_fqdn = controlling_node_fqdn
        self.force = force
        self.spicerack = spicerack
        self.task_id = task_id

    def run(self) -> Optional[int]:
        """Main entry point"""
        controller = CephController(remote=self.spicerack.remote(), controlling_node_fqdn=self.controlling_node_fqdn)
        osd_nodes = list(controller.get_nodes()["osd"].keys())

        dologmsg(project="admin", message=f"Rebooting the nodes {','.join(osd_nodes)}", task_id=self.task_id)

        controller.set_maintenance()

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, osd_node in enumerate(osd_nodes):
            LOGGER.info("Rebooting node %s, %d done, %d to go", osd_node, index, len(osd_nodes) - index)
            args = [
                "--skip-maintenance",
                "--controlling-node-fqdn",
                self.controlling_node_fqdn,
                "--fqdn-to-reboot",
                f"{osd_node}.{controller.get_nodes_domain()}",
            ]
            if self.force:
                args.append("--force")
            if self.task_id:
                args.extend(["--task-id", self.task_id])

            reboot_node_cookbook.get_runner(args=reboot_node_cookbook.argument_parser().parse_args(args)).run()
            LOGGER.info(
                "Rebooted node %s, %d done, %d to go, waiting for cluster to stabilize...",
                osd_node,
                index + 1,
                len(osd_nodes) - index - 1,
            )
            controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        controller.unset_maintenance()
        dologmsg(project="admin", message=f"Finished rebooting the nodes {osd_nodes}", task_id=self.task_id)
