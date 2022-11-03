r"""WMCS Ceph - Reboot a single ceph node.

Usage example:
    cookbook wmcs.ceph.reboot_node \
        --fqdn-to-reboot cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import datetime
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.alerts import downtime_host, uptime_host
from cookbooks.wmcs.libs.ceph import CephClusterController, get_node_cluster_name
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts

LOGGER = logging.getLogger(__name__)


class RebootNode(CookbookBase):
    """WMCS Ceph cookbook to a node of the cluster."""

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
            "--fqdn-to-reboot",
            required=True,
            help="FQDN of the node to reboot.",
        )
        parser.add_argument(
            "--skip-maintenance",
            required=False,
            default=False,
            action="store_true",
            help="If passed, will not set the cluster in maintenance mode (careful! might start rebalancing data).",
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
        return with_common_opts(self.spicerack, args, RebootNodeRunner,)(
            fqdn_to_reboot=args.fqdn_to_reboot,
            skip_maintenance=args.skip_maintenance,
            force=args.force,
            spicerack=self.spicerack,
        )


class RebootNodeRunner(WMCSCookbookRunnerBase):
    """Runner for RebootNode"""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn_to_reboot: str,
        force: bool,
        skip_maintenance: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.fqdn_to_reboot = fqdn_to_reboot
        self.skip_maintenance = skip_maintenance
        self.force = force
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(),
            cluster_name=get_node_cluster_name(node=self.fqdn_to_reboot),
            spicerack=self.spicerack,
        )

    def run_with_proxy(self) -> None:
        """Main entry point"""
        self.sallogger.log(message=f"Rebooting node {self.fqdn_to_reboot}")

        if not self.force:
            self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)

        if not self.skip_maintenance:
            silences = self.controller.set_maintenance(task_id=self.common_opts.task_id, reason="Rebooting node")
        else:
            silences = []

        node = self.spicerack.remote().query(f"D{{{self.fqdn_to_reboot}}}", use_sudo=True)
        host_name = self.fqdn_to_reboot.split(".", 1)[0]
        silence_id = downtime_host(
            spicerack=self.spicerack,
            host_name=host_name,
            comment="Rebooting with wmcs.ceph.reboot_node",
            task_id=self.common_opts.task_id,
        )
        reboot_time = datetime.datetime.utcnow()
        node.reboot()

        node.wait_reboot_since(since=reboot_time)
        LOGGER.info(
            "Rebooted node %s, waiting for cluster to stabilize...",
            self.fqdn_to_reboot,
        )
        if self.force:
            LOGGER.info("Force passed, ignoring cluster health and continuing")
        else:
            self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
            LOGGER.info("Cluster stable, continuing")

        if not self.skip_maintenance:
            self.controller.unset_maintenance(silences=silences)

        uptime_host(spicerack=self.spicerack, host_name=host_name, silence_id=silence_id)

        self.sallogger.log(message=f"Finished rebooting node {self.fqdn_to_reboot}")
