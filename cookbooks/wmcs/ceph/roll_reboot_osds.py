"""WMCS Ceph - Rolling reboot of all the osd nodes.

Usage example:
    cookbook wmcs.ceph.roll_reboot_osds \
        --controlling-node-fqdn cloudcephmon2001-dev.codfw.wmnet

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.ceph.reboot_node import RebootNode
from cookbooks.wmcs.libs.ceph import CephClusterController

LOGGER = logging.getLogger(__name__)


class RollRebootOsds(CookbookBase):
    """WMCS Ceph cookbook to rolling reboot all osds."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
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
        return with_common_opts(self.spicerack, args, RollRebootOsdsRunner,)(
            controlling_node_fqdn=args.controlling_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class RollRebootOsdsRunner(CookbookRunnerBase):
    """Runner for RollRebootOsds"""

    def __init__(
        self,
        common_opts: CommonOpts,
        controlling_node_fqdn: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.controlling_node_fqdn = controlling_node_fqdn
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.controlling_node_fqdn, spicerack=self.spicerack
        )
        osd_nodes = list(controller.get_nodes()["osd"].keys())

        self.sallogger.log(message=f"Rebooting the nodes {','.join(osd_nodes)}")

        silences = controller.set_maintenance(reason="Roll rebooting OSDs")

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, osd_node in enumerate(osd_nodes):
            LOGGER.info("Rebooting node %s, %d done, %d to go", osd_node, index, len(osd_nodes) - index)
            args = [
                "--skip-maintenance",
                "--controlling-node-fqdn",
                self.controlling_node_fqdn,
                "--fqdn-to-reboot",
                f"{osd_node}.{controller.get_nodes_domain()}",
            ] + self.common_opts.to_cli_args()

            if self.force:
                args.append("--force")

            reboot_node_cookbook.get_runner(args=reboot_node_cookbook.argument_parser().parse_args(args)).run()
            LOGGER.info(
                "Rebooted node %s, %d done, %d to go, waiting for cluster to stabilize...",
                osd_node,
                index + 1,
                len(osd_nodes) - index - 1,
            )
            controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        controller.unset_maintenance(silences=silences)
        self.sallogger.log(message=f"Finished rebooting the nodes {osd_nodes}")
