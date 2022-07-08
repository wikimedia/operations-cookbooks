"""WMCS Ceph - Set cluster in maintenance.

Usage example:
    cookbook wmcs.ceph.set_cluster_in_maintenance \
        --monitor-node-fqdn cloudcephosd2001-dev.codfw.wmnet \
        --reason "Doing some tests or similar"

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.openstack import Deployment

LOGGER = logging.getLogger(__name__)


class SetClusterInMaintenance(CookbookBase):
    """WMCS Ceph cookbook to set a cluster in maintenance."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--monitor-node-fqdn",
            required=True,
            help="FQDN of one of the monitor nodes to manage the cluster.",
        )
        parser.add_argument(
            "--reason",
            required=True,
            help="Reason for the maintenance.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )
        add_common_opts(parser)

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(spicerack=self.spicerack, args=args, runner=SetClusterInMaintenanceRunner)(
            monitor_node_fqdn=args.monitor_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
            reason=args.reason,
        )


class SetClusterInMaintenanceRunner(CookbookRunnerBase):
    """Runner for SetClusterInMaintenance"""

    def __init__(
        self,
        monitor_node_fqdn: str,
        force: bool,
        spicerack: Spicerack,
        common_opts: CommonOpts,
        reason: str,
    ):
        """Init"""
        self.monitor_node_fqdn = monitor_node_fqdn
        self.force = force
        self.reason = reason
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.monitor_node_fqdn, spicerack=self.spicerack
        )

    def run(self) -> None:
        """Main entry point"""
        deployment = Deployment.get_for_node(self.monitor_node_fqdn)
        silences = self.controller.set_maintenance(force=self.force, reason=self.reason)
        self.sallogger.log(
            f"Set the ceph cluster for {deployment} in maintenance, alert silence ids: {','.join(silences)}"
        )
