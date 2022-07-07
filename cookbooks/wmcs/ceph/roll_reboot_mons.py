"""WMCS Ceph - Rolling reboot of all the mon nodes.

Usage example:
    cookbook wmcs.ceph.roll_reboot_mons \
        --controlling-node-fqdn cloudcephmon2001-dev.codfw.wmnet

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.ceph.reboot_node import RebootNode
from cookbooks.wmcs.lib.ceph import CephClusterController

LOGGER = logging.getLogger(__name__)


class RollRebootMons(CookbookBase):
    """WMCS Ceph cookbook to rolling reboot all mons."""

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
        return with_common_opts(self.spicerack, args, RollRebootMonsRunner,)(
            controlling_node_fqdn=args.controlling_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
        )


class RollRebootMonsRunner(CookbookRunnerBase):
    """Runner for RollRebootMons"""

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
        mon_nodes = list(controller.get_nodes()["mon"].keys())

        self.sallogger.log(message=f"Rebooting the nodes {','.join(mon_nodes)}")

        silences = controller.set_maintenance(task_id=self.common_opts.task_id, reason="Roll rebooting mons")

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, mon_node in enumerate(mon_nodes):
            if mon_node == self.controlling_node_fqdn:
                controller.change_controlling_node()

            LOGGER.info("Rebooting node %s, %d done, %d to go", mon_node, index, len(mon_nodes) - index)
            args = [
                "--skip-maintenance",
                "--controlling-node-fqdn",
                self.controlling_node_fqdn,
                "--fqdn-to-reboot",
                f"{mon_node}.{controller.get_nodes_domain()}",
            ] + self.common_opts.to_cli_args()

            if self.force:
                args.append("--force")

            reboot_node_cookbook.get_runner(args=reboot_node_cookbook.argument_parser().parse_args(args)).run()
            LOGGER.info(
                "Rebooted node %s, %d done, %d to go, waiting for cluster to stabilize...",
                mon_node,
                index + 1,
                len(mon_nodes) - index - 1,
            )
            controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        controller.unset_maintenance(silences=silences)

        self.sallogger.log(message=f"Finished rebooting the nodes {mon_nodes}")
