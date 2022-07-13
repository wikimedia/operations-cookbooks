"""WMCS Ceph - Unset cluster maintenance.

Usage example:
    cookbook wmcs.ceph.unset_cluster_maintenance \
        --monitor-node-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import logging
from typing import List, Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.alerts import SilenceID
from cookbooks.wmcs.libs.ceph import CephClusterController
from cookbooks.wmcs.libs.openstack.common import Deployment

LOGGER = logging.getLogger(__name__)


class UnSetClusterInMaintenance(CookbookBase):
    """WMCS Ceph cookbook to unset a cluster maintenance."""

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
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )
        parser.add_argument(
            "--silence-ids",
            required=False,
            default=None,
            type=lambda silences_str: [silence.strip() for silence in silences_str.split(",")],
            help=(
                "Comma separated list of silences to unmute. If not passed will unmute all the silences affecting the "
                "ceph cluster alerts."
            ),
        )
        add_common_opts(parser)

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(spicerack=self.spicerack, args=args, runner=UnSetClusterInMaintenanceRunner)(
            monitor_node_fqdn=args.monitor_node_fqdn,
            force=args.force,
            spicerack=self.spicerack,
            silence_ids=args.silence_ids,
        )


class UnSetClusterInMaintenanceRunner(CookbookRunnerBase):
    """Runner for UnSetClusterInMaintenance"""

    def __init__(
        self,
        monitor_node_fqdn: str,
        force: bool,
        spicerack: Spicerack,
        common_opts: CommonOpts,
        silence_ids: Optional[List[SilenceID]],
    ):
        """Init"""
        self.monitor_node_fqdn = monitor_node_fqdn
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.monitor_node_fqdn, spicerack=self.spicerack
        )
        self.silence_ids = silence_ids
        self.deployment = Deployment.get_for_node(self.monitor_node_fqdn)

    def run(self) -> None:
        """Main entry point"""
        self.controller.unset_maintenance(force=self.force, silences=self.silence_ids)
        self.sallogger.log("Ceph cluster at {self.deployment} set out of maintenance.")
