r"""WMCS Ceph - Rolling reboot of all the osd nodes.

Usage example:
    cookbook wmcs.ceph.roll_reboot_osds \
        --cluster-name eqiad1

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.ceph.reboot_node import RebootNode
from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import CephClusterName

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
            "--cluster-name",
            required=True,
            choices=list(CephClusterName),
            type=CephClusterName,
            help="Ceph cluster to roll reboot.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RollRebootOsdsRunner)(
            cluster_name=args.cluster_name,
            force=args.force,
            spicerack=self.spicerack,
        )


class RollRebootOsdsRunner(WMCSCookbookRunnerBase):
    """Runner for RollRebootOsds"""

    def __init__(
        self,
        common_opts: CommonOpts,
        cluster_name: CephClusterName,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.force = force
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )

    def run_with_proxy(self) -> None:
        """Main entry point"""
        osd_nodes = list(self.controller.get_nodes()["osd"].keys())

        self.sallogger.log(message=f"Rebooting the nodes {','.join(osd_nodes)}")

        silences = self.controller.set_maintenance(reason="Roll rebooting OSDs")

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, osd_node in enumerate(osd_nodes):
            LOGGER.info("Rebooting node %s, %d done, %d to go", osd_node, index, len(osd_nodes) - index)
            args = [
                "--skip-maintenance",
                "--fqdn-to-reboot",
                f"{osd_node}.{self.controller.get_nodes_domain()}",
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
            self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        self.controller.unset_maintenance(silences=silences)
        self.sallogger.log(message=f"Finished rebooting the nodes {osd_nodes}")
