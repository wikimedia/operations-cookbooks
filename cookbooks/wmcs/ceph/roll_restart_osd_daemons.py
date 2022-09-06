r"""WMCS Ceph - Rolling restart all the osd daemons (not nodes).

Usage example:
    cookbook wmcs.ceph.roll_restart_osd_daemons \
        --cluster-name eqiad1 \
        --interactive

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation

from cookbooks.wmcs.libs.ceph import CephClusterController, CephClusterUnhealthy
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, run_one_raw, with_common_opts
from cookbooks.wmcs.libs.inventory import CephClusterName

LOGGER = logging.getLogger(__name__)


class RollRestartOsdDaemons(CookbookBase):
    """WMCS Ceph cookbook to rolling restart all osd daemons."""

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
        parser.add_argument(
            "--interactive",
            required=False,
            action="store_true",
            help="If passed, it will ask for confirmation before restarting the OSD daemons for each node.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RollRebootOsdsRunner,)(
            cluster_name=args.cluster_name,
            force=args.force,
            interactive=args.interactive,
            spicerack=self.spicerack,
        )


class RollRebootOsdsRunner(CookbookRunnerBase):
    """Runner for RollRebootOsds"""

    def __init__(
        self,
        common_opts: CommonOpts,
        cluster_name: CephClusterName,
        force: bool,
        interactive: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.interactive = interactive
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )

    def run(self) -> None:
        """Main entry point"""
        osd_nodes = list(self.controller.get_nodes()["osd"].keys())

        self.sallogger.log(message=f"Restarting the osd daemons from nodes {','.join(osd_nodes)}")

        silences = self.controller.set_maintenance(reason="Roll restarting  OSD daemons")

        for index, osd_node in enumerate(osd_nodes):
            if self.interactive:
                ask_confirmation(f"Ready to restart the OSD daemons for node {osd_node}?")

            LOGGER.info("Restarting osds from node %s, %d done, %d to go", osd_node, index, len(osd_nodes) - index)
            remote_node = self.spicerack.remote().query(
                f"D{{{osd_node}.{self.controller.get_nodes_domain()}}}", use_sudo=True
            )
            run_one_raw(command=["systemctl", "restart", "ceph-osd@*"], node=remote_node)

            LOGGER.info(
                "Restarted OSD daemons on node %s, %d done, %d to go, waiting for cluster to stabilize...",
                osd_node,
                index + 1,
                len(osd_nodes) - index - 1,
            )
            try:
                self.controller.wait_for_cluster_healthy(consider_maintenance_healthy=True)
                LOGGER.info("Cluster stable, continuing")
            except CephClusterUnhealthy:
                if self.force:
                    LOGGER.warning("Cluster is not stable, but force was passed, continuing...")
                else:
                    raise

        self.controller.unset_maintenance(silences=silences)
        self.sallogger.log(message=f"Finished restarting all the OSD daemons from the nodes {osd_nodes}")
