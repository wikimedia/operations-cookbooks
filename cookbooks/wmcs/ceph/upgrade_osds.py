r"""WMCS Ceph - Upgrade all the osd nodes.

Usage example:
    cookbook wmcs.ceph.upgrade_osds \
        --cluster-name eqiad1

"""
import argparse
import logging
from typing import List, Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.ceph.upgrade_ceph_node import UpgradeCephNode
from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import CephClusterName

LOGGER = logging.getLogger(__name__)


class UpgradeOsds(CookbookBase):
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
            help="Ceph cluster to unset the maintenance of.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )
        parser.add_argument(
            "--osd-nodes",
            required=False,
            default="",
            type=lambda csl: csl.split(",") if csl else [],
            help=(
                "Comma separated list of osds to upgrade (hostnames, not fqdn), if none passed, will upgrade all "
                "the ones currently in the cluster. Example: cloudcephosd1021,cloudcephosd1033"
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, UpgradeOsdsRunner)(
            cluster_name=args.cluster_name,
            osd_nodes=args.osd_nodes,
            force=args.force,
            spicerack=self.spicerack,
        )


class UpgradeOsdsRunner(CookbookRunnerBase):
    """Runner for UpgradeOsds"""

    def __init__(
        self,
        cluster_name: CephClusterName,
        force: bool,
        common_opts: CommonOpts,
        spicerack: Spicerack,
        osd_nodes: Optional[List[str]],
    ):
        """Init"""
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )
        self.osd_nodes = osd_nodes or []

    def run(self) -> None:
        """Main entry point"""
        silences = self.controller.set_maintenance(reason="Upgrading osds")

        upgrade_ceph_node_cookbook = UpgradeCephNode(spicerack=self.spicerack)
        if not self.osd_nodes:
            osd_nodes = list(self.controller.get_nodes()["osd"].keys())
        else:
            osd_nodes = self.osd_nodes

        self.sallogger.log(f"Upgrading OSDs and rebooting the nodes {osd_nodes}")

        for index, osd_node in enumerate(osd_nodes):
            LOGGER.info("Upgrading node %s, %d done, %d to go", osd_node, index, len(osd_nodes) - index)
            args = [
                "--to-upgrade-fqdn",
                f"{osd_node}.{self.controller.get_nodes_domain()}",
                "--skip-maintenance",
            ]
            if self.force:
                args.append("--force")

            upgrade_ceph_node_cookbook.get_runner(
                args=upgrade_ceph_node_cookbook.argument_parser().parse_args(args)
            ).run()
            LOGGER.info(
                "Upgraded node %s, %d done, %d to go, waiting for cluster to stabilize...",
                osd_node,
                index + 1,
                len(osd_nodes) - index - 1,
            )
            self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        self.controller.unset_maintenance(silences=silences)
        self.sallogger.log(f"OSDs ({osd_nodes}) upgraded successfully B-)")
