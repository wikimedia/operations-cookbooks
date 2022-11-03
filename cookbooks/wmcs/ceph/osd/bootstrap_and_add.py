r"""WMCS Ceph - Bootstrap a new osd

Usage example:
    cookbook wmcs.ceph.osd.bootstrap_and_add \
        --new-osd-fqdn cloudcephosd1016.eqiad.wmnet \
        --task-id T12345

"""
# pylint: disable=too-many-arguments
import argparse
import logging
import time
from typing import List

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs.ceph.reboot_node import RebootNode
from cookbooks.wmcs.libs.ceph import (
    CephClusterController,
    CephOSDFlag,
    CephOSDNodeController,
    OSDClass,
    OSDTreeEntry,
    get_node_cluster_name,
)
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts

LOGGER = logging.getLogger(__name__)


class BootstrapAndAdd(CookbookBase):
    """WMCS Ceph cookbook to bootstrap and add a new OSD."""

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
            "--new-osd-fqdn",
            required=True,
            action="append",
            help=(
                "FQDNs of the new OSDs to add. Repeat for each new OSD. If specifying more than one, consider passing "
                "--yes-i-know-what-im-doing"
            ),
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
            "--only-check",
            required=False,
            action="store_true",
            help="If passed, will only run the pre-setup checks on the host and report back, nothing more.",
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
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, BootstrapAndAddRunner)(
            new_osd_fqdns=args.new_osd_fqdn,
            yes_i_know=args.yes_i_know_what_im_doing,
            skip_reboot=args.skip_reboot,
            wait_for_rebalance=args.wait_for_rebalance,
            force=args.force,
            only_check=args.only_check,
            spicerack=self.spicerack,
        )


def _wait_for_osds_to_show_up(cluster_controller: CephClusterController, ceph_hostname: str) -> List[OSDTreeEntry]:
    osd_tree = cluster_controller.get_osd_tree()
    retries = 0
    while not cluster_controller.is_osd_host_valid(osd_tree=osd_tree, hostname=ceph_hostname):
        time.sleep(5)
        retries += 1
        if retries > 10:
            raise Exception(f"The new OSD node ({ceph_hostname}) is not in the OSD tree, or is not as expected")
        osd_tree = cluster_controller.get_osd_tree()

    LOGGER.info("All OSDs are showing up in the cluster, continuing.")
    host_node = next(node for node in osd_tree["nodes"]["children"] if node["name"] == ceph_hostname)
    return host_node["children"]


class BootstrapAndAddRunner(WMCSCookbookRunnerBase):
    """Runner for BootstrapAndAdd"""

    def __init__(
        self,
        common_opts: CommonOpts,
        new_osd_fqdns: List[str],
        force: bool,
        yes_i_know: bool,
        skip_reboot: bool,
        wait_for_rebalance: bool,
        only_check: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.new_osd_fqdns = new_osd_fqdns
        self.force = force
        self.yes_i_know = yes_i_know
        self.skip_reboot = skip_reboot
        super().__init__(spicerack=spicerack)
        self.wait_for_rebalance = wait_for_rebalance
        self.only_check = only_check
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        cluster_name = get_node_cluster_name(self.new_osd_fqdns[0])
        self.cluster_controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )

    def run_with_proxy(self) -> None:
        """Main entry point"""
        self.sallogger.log(
            message=f"Adding new OSDs {self.new_osd_fqdns} to the cluster",
        )
        if not self.only_check:
            # this avoids rebalancing after each osd is added
            self.cluster_controller.set_osdmap_flag(CephOSDFlag("norebalance"))

        for index, new_osd_fqdn in enumerate(self.new_osd_fqdns):
            self.sallogger.log(
                message=f"Adding OSD {new_osd_fqdn}... ({index + 1}/{len(self.new_osd_fqdns)})",
            )
            node = self.spicerack.remote().query(f"D{{{new_osd_fqdn}}}", use_sudo=True)
            osd_controller = CephOSDNodeController(remote=self.spicerack.remote(), node_fqdn=new_osd_fqdn)

            if not self.skip_reboot:
                LOGGER.info("Running puppet and rebooting to make sure we start from fresh boot.")
                PuppetHosts(remote_hosts=node).run()
                reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
                reboot_args = [
                    "--skip-maintenance",
                    "--fqdn-to-reboot",
                    new_osd_fqdn,
                ]
                if self.force:
                    reboot_args += ["--force"]

                reboot_args += self.common_opts.to_cli_args()

                reboot_node_cookbook.get_runner(
                    args=reboot_node_cookbook.argument_parser().parse_args(reboot_args)
                ).run()
                # Puppet adds the network routes to the cluster network on run
                # so we need to run it once after reboot
                PuppetHosts(remote_hosts=node).run()

            LOGGER.info("Doing some checks...")
            node_failures = self.cluster_controller.check_if_osd_ready_for_bootstrap(osd_controller=osd_controller)
            if node_failures:
                errors_str = "\n    ".join(node_failures)
                error_msg = f"The node {new_osd_fqdn} is not suitable to be added as an osd:\n    {errors_str}"
                LOGGER.error(error_msg)
                raise Exception(error_msg)
            LOGGER.info("...OK")

            if self.only_check:
                continue

            osd_controller.add_all_available_devices(interactive=(not self.yes_i_know))

            new_osds = _wait_for_osds_to_show_up(
                cluster_controller=self.cluster_controller, ceph_hostname=new_osd_fqdn.split(".", 1)[0]
            )
            wrongly_classified_osds = [osd for osd in new_osds if osd.device_class != OSDClass.SSD]
            if wrongly_classified_osds:
                LOGGER.info("Got some OSDs with the wrong classes, fixing:%s", wrongly_classified_osds)
            for osd in wrongly_classified_osds:
                self.cluster_controller.set_osd_class(osd_id=osd.osd_id, osd_class=OSDClass.SSD)

            new_osds = _wait_for_osds_to_show_up(
                cluster_controller=self.cluster_controller, ceph_hostname=new_osd_fqdn.split(".", 1)[0]
            )
            wrongly_classified_osds = [osd for osd in new_osds if osd.device_class != OSDClass.SSD]
            if wrongly_classified_osds:
                raise Exception(
                    f"Something went wrong, I was unable to change the device class for osds {wrongly_classified_osds}"
                )

            self.sallogger.log(
                message=f"Added OSD {new_osd_fqdn}... ({index + 1}/{len(self.new_osd_fqdns)})",
            )

        if self.only_check:
            return

        # Now we start rebalancing once all are in
        self.cluster_controller.unset_osdmap_flag(CephOSDFlag("norebalance"))
        self.sallogger.log(
            message=f"Added {len(self.new_osd_fqdns)} new OSDs {self.new_osd_fqdns}",
        )
        LOGGER.info(
            "The new OSDs are up and running, the cluster will now start rebalancing the data to them, that might "
            "take quite a long time, you can follow the progress by running 'ceph status' on a control node."
        )

        if self.wait_for_rebalance:
            # the rebalance might take a very very long time, setting timeout to 12h
            wait_hours = 12
            LOGGER.info("Waiting for the cluster to rebalance all the data (timeout of {%d} hours)...", wait_hours)
            self.cluster_controller.wait_for_in_progress_events(timeout_seconds=wait_hours * 60 * 60)
            LOGGER.info("Rebalancing done.")
            self.sallogger.log(
                message=f"The cluster is now rebalanced after adding the new OSDs {self.new_osd_fqdns}",
            )
