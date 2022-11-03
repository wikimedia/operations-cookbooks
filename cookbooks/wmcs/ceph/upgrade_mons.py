r"""WMCS Ceph - Upgrade all the mon nodes.

Usage example:
    cookbook wmcs.ceph.upgrade_mons \
        --cluster-name eqiad1

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.ceph.upgrade_ceph_node import UpgradeCephNode
from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import CephClusterName

LOGGER = logging.getLogger(__name__)


class UpgradeMons(CookbookBase):
    """WMCS Ceph cookbook to set a cluster in maintenance."""

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
        return with_common_opts(self.spicerack, args, UpgradeMonsRunner)(
            cluster_name=args.cluster_name,
            force=args.force,
            spicerack=self.spicerack,
        )


class UpgradeMonsRunner(WMCSCookbookRunnerBase):
    """Runner for UpgradeMons"""

    def __init__(
        self,
        cluster_name: CephClusterName,
        force: bool,
        common_opts: CommonOpts,
        spicerack: Spicerack,
    ):
        """Init"""
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
        silences = self.controller.set_maintenance(reason="Upgrading mon nodes.")

        upgrade_ceph_node_cookbook = UpgradeCephNode(spicerack=self.spicerack)
        monitor_nodes = list(self.controller.get_nodes()["mon"].keys())
        self.sallogger.log(f"Upgrading MONs and rebooting the nodes {monitor_nodes}")

        for index, monitor_node in enumerate(monitor_nodes):
            LOGGER.info("Upgrading node %s, %d done, %d to go", monitor_node, index, len(monitor_nodes) - index)
            args = [
                "--to-upgrade-fqdn",
                f"{monitor_node}.{self.controller.get_nodes_domain()}",
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
            self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        self.controller.unset_maintenance(silences=silences)
        self.sallogger.log(f"MONs ({monitor_nodes}) upgraded successfully B-)")
