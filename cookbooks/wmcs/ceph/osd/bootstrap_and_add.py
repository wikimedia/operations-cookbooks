"""WMCS Ceph - Bootstrap a new osd

Usage example:
    cookbook wmcs.ceph.osd.boostrap_and_add \
        --new-osd-fqdn cloudcephosd1016.eqiad.wmnet \
        --task-id T12345

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import List, Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs import CephClusterController, CephOSDController, dologmsg
from cookbooks.wmcs.ceph.reboot_node import RebootNode

LOGGER = logging.getLogger(__name__)
# This will not be used for the OSD
OS_DEVICES = ["sda", "sdb"]


class BootstrapAndAdd(CookbookBase):
    """WMCS Ceph cookbook to bootsrap and add a new OSD."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--new-osd-fqdn",
            required=True,
            action="append",
            help=(
                "FQDNs of the new OSDs to add. Repeat for each new OSD. If specifying more than one, consider passing "
                "--yes-i-know-what-im-doing"
            ),
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this reboot (ex. T123456)",
        )
        parser.add_argument(
            "--controlling-node-fqdn",
            required=True,
            help="FQDN of one of the existing nodes to manage the cluster.",
        )
        parser.add_argument(
            "--skip-reboot",
            required=False,
            action="store_true",
            help=(
                "If passed, will not do the first reboot before adding the new osds. Useful when the machine has "
                "already some running OSDs and you are sure the reboot is not needed."
            ),
        )
        parser.add_argument(
            "--yes-i-know-what-im-doing",
            required=False,
            action="store_true",
            help=(
                "If passed, will not ask for confirmation. WARNING: this might cause data loss, use only when you are "
                "sure what you are doing."
            ),
        )
        parser.add_argument(
            "--wait-for-rebalance",
            required=False,
            action="store_true",
            help=(
                "If passed, will wait for the cluster to do the rebalancing after adding the new OSDs. Note that this "
                "might take several hours."
            ),
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return BootstrapAndAddRunner(
            new_osd_fqdns=args.new_osd_fqdn,
            yes_i_know=args.yes_i_know_what_im_doing,
            skip_reboot=args.skip_reboot,
            wait_for_rebalance=args.wait_for_rebalance,
            controlling_node_fqdn=args.controlling_node_fqdn,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class BootstrapAndAddRunner(CookbookRunnerBase):
    """Runner for BootstrapAndAdd"""

    def __init__(
        self,
        new_osd_fqdns: List[str],
        yes_i_know: bool,
        skip_reboot: bool,
        wait_for_rebalance: bool,
        task_id: str,
        controlling_node_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.new_osd_fqdns = new_osd_fqdns
        self.controlling_node_fqdn = controlling_node_fqdn
        self.yes_i_know = yes_i_know
        self.skip_reboot = skip_reboot
        self.spicerack = spicerack
        self.wait_for_rebalance = wait_for_rebalance
        self.task_id = task_id

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(
            project="admin",
            message=f"Adding new OSDs {self.new_osd_fqdns} to the cluster",
            task_id=self.task_id,
        )
        cluster_controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.controlling_node_fqdn
        )
        # this avoids rebalancing after each osd is added
        cluster_controller.set_osdmap_flag("norebalance")

        for index, new_osd_fqdn in enumerate(self.new_osd_fqdns):
            dologmsg(
                project="admin",
                message=f"  Adding OSD {new_osd_fqdn}... ({index + 1}/{len(self.new_osd_fqdns)})",
                task_id=self.task_id,
            )
            node = self.spicerack.remote().query(f"D{{{new_osd_fqdn}}}", use_sudo=True)
            # make sure puppet has run fully
            PuppetHosts(remote_hosts=node).run()
            if not self.skip_reboot:
                # make sure to start from fresh boot
                reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
                reboot_args = [
                    "--skip-maintenance",
                    "--controlling-node-fqdn",
                    self.controlling_node_fqdn,
                    "--fqdn-to-reboot",
                    new_osd_fqdn,
                ]
                reboot_node_cookbook.get_runner(
                    args=reboot_node_cookbook.argument_parser().parse_args(reboot_args)
                ).run()

            CephOSDController(remote=self.spicerack.remote(), node_fqdn=new_osd_fqdn).add_all_available_devices(
                interactive=(not self.yes_i_know)
            )
            dologmsg(
                project="admin",
                message=f"  Added OSD {new_osd_fqdn}... ({index + 1}/{len(self.new_osd_fqdns)})",
                task_id=self.task_id,
            )

        # Now we start rebalancing once all are in
        cluster_controller.unset_osdmap_flag("norebalance")
        dologmsg(
            project="admin",
            message=f"Added {len(self.new_osd_fqdns)} new OSDs {self.new_osd_fqdns}",
            task_id=self.task_id,
        )
        LOGGER.info(
            "The new OSDs are up and running, the cluster will now start rebalancing the data to them, that might "
            "take quite a long time, you can follow the progress by running 'ceph status' on a control node."
        )

        if self.wait_for_rebalance:
            # the rebalance might take a very very long time, setting timeout to 12h
            wait_hours = 12
            LOGGER.info("Waiting for the cluster to rebalance all the data (timeout of {%d} hours)...", wait_hours)
            cluster_controller.wait_for_in_progress_events(timeout_seconds=wait_hours * 60 * 60)
            LOGGER.info("Rebalancing done.")
            dologmsg(
                project="admin",
                message=f"The cluster is now rebalanced after adding the new OSDs {self.new_osd_fqdns}",
                task_id=self.task_id,
            )
