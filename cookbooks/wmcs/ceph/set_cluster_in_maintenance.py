r"""WMCS Ceph - Set cluster in maintenance.

Usage example:
    cookbook wmcs.ceph.set_cluster_in_maintenance \
        --cluster-name eqiad1 \
        --reason "Doing some tests or similar"

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import CephClusterName

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
            "--cluster-name",
            required=True,
            choices=list(CephClusterName),
            type=CephClusterName,
            help="Ceph cluster to set in maintenance.",
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
        cluster_name: CephClusterName,
        force: bool,
        spicerack: Spicerack,
        common_opts: CommonOpts,
        reason: str,
    ):
        """Init"""
        self.cluster_name = cluster_name
        self.force = force
        self.reason = reason
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )

    def run(self) -> None:
        """Main entry point"""
        silences = self.controller.set_maintenance(force=self.force, reason=self.reason)
        self.sallogger.log(
            f"Set the ceph cluster for {self.cluster_name} in maintenance, alert silence ids: {','.join(silences)}"
        )
