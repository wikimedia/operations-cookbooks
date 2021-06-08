"""WMCS Ceph - Reboot a single osd node.

Usage example:
    cookbook wmcs.ceph.reboot_osd \
        --controlling-node-fqdn cloudcephmon2001-dev.codfw.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import datetime
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CephController, dologmsg, wrap_with_sudo_icinga

LOGGER = logging.getLogger(__name__)


class RebootOsd(CookbookBase):
    """WMCS Ceph cookbook to reboot an osd."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--fqdn-to-reboot",
            required=True,
            help="FQDN of the node to reboot.",
        )
        parser.add_argument(
            "--controlling-node-fqdn",
            required=True,
            help="FQDN of one of the nodes to manage the cluster.",
        )
        parser.add_argument(
            "--skip-maintenance",
            required=False,
            default=False,
            action="store_true",
            help="If passed, will not set the cluster in maintenance mode (careful! might start rebalancing data).",
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
        return RebootOsdRunner(
            fqdn_to_reboot=args.fqdn_to_reboot,
            controlling_node_fqdn=args.controlling_node_fqdn,
            task_id=args.task_id,
            skip_maintenance=args.skip_maintenance,
            force=args.force,
            spicerack=self.spicerack,
        )


class RebootOsdRunner(CookbookRunnerBase):
    """Runner for RebootOsd"""

    def __init__(
        self,
        fqdn_to_reboot: str,
        controlling_node_fqdn: str,
        task_id: str,
        force: bool,
        skip_maintenance: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.fqdn_to_reboot = fqdn_to_reboot
        self.controlling_node_fqdn = controlling_node_fqdn
        self.skip_maintenance = skip_maintenance
        self.force = force
        self.task_id = task_id
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(project="admin", message=f"Rebooting node {self.fqdn_to_reboot}", task_id=self.task_id)

        controller = CephController(remote=self.spicerack.remote(), controlling_node_fqdn=self.controlling_node_fqdn)
        if not self.force:
            controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)

        if not self.skip_maintenance:
            controller.set_maintenance()

        node = self.spicerack.remote().query(f"D{{{self.fqdn_to_reboot}}}", use_sudo=True)
        icinga = wrap_with_sudo_icinga(my_spicerack=self.spicerack).icinga()
        icinga.downtime_hosts(
            hosts=node.hosts,
            reason=self.spicerack.admin_reason(
                reason="Rebooting at user request through cookbook", task_id=self.task_id
            ),
            duration=datetime.timedelta(minutes=20),
        )

        reboot_time = datetime.datetime.utcnow()
        node.reboot()

        node.wait_reboot_since(since=reboot_time)
        LOGGER.info(
            "Rebooted node %s, waiting for cluster to stabilize...",
            self.fqdn_to_reboot,
        )
        controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
        LOGGER.info("Cluster stable, continuing")

        if not self.skip_maintenance:
            controller.unset_maintenance()

        icinga.remove_downtime(hosts=node.hosts)
        dologmsg(project="admin", message=f"Finished rebooting node {self.fqdn_to_reboot}", task_id=self.task_id)
