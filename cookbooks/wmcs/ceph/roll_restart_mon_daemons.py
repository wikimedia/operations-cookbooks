r"""WMCS Ceph - Rolling restart all the mon daemons (not nodes).

Usage example:
    cookbook wmcs.ceph.roll_restart_mon_daemons \
        --cluster-name eqiad1 \
        --interactive

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase
from wmflib.interactive import ask_confirmation

from cookbooks.wmcs.libs.ceph import CephClusterController, CephClusterUnhealthy
from cookbooks.wmcs.libs.common import (
    CommonOpts,
    SALLogger,
    WMCSCookbookRunnerBase,
    add_common_opts,
    run_one_raw,
    with_common_opts,
)
from cookbooks.wmcs.libs.inventory import CephClusterName

LOGGER = logging.getLogger(__name__)


class RollRestartMonDaemons(CookbookBase):
    """WMCS Ceph cookbook to rolling restart all Mon daemons."""

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
            help="Ceph cluster to roll restart.",
        )
        parser.add_argument(
            "--ignore-current-health-issues",
            required=False,
            action="store_true",
            help=(
                "If passed, will ignore any health issues that are happening already when checking the cluster "
                "health. Useful when the cluster is not in an optimal state when rebooting the daemons but you don't "
                "want to break it even more while doing so. Prefer this to --force if you are unsure which one to use."
            ),
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
            help="If passed, it will ask for confirmation before restarting the Mon daemons for each node.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RollRestartMonDaemonsRunner,)(
            cluster_name=args.cluster_name,
            ignore_current_health_issues=args.ignore_current_health_issues,
            force=args.force,
            interactive=args.interactive,
            spicerack=self.spicerack,
        )


class RollRestartMonDaemonsRunner(WMCSCookbookRunnerBase):
    """Runner for RollRestartMonDaemons"""

    def __init__(
        self,
        common_opts: CommonOpts,
        cluster_name: CephClusterName,
        ignore_current_health_issues: bool,
        force: bool,
        interactive: bool,
        spicerack: Spicerack,
    ):  # pylint: disable=too-many-arguments
        """Init"""
        self.common_opts = common_opts
        self.force = force
        self.ignore_current_health_issues = ignore_current_health_issues
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.interactive = interactive
        self.controller = CephClusterController(
            remote=self.spicerack.remote(), cluster_name=cluster_name, spicerack=self.spicerack
        )

    def run_with_proxy(self) -> None:
        """Main entry point"""
        mon_nodes = list(self.controller.get_nodes()["mon"].keys())

        self.sallogger.log(message=f"Restarting the mon daemons from nodes {','.join(mon_nodes)}")

        silences = self.controller.set_maintenance(
            reason="Roll restarting mon daemons", force=self.force or self.ignore_current_health_issues
        )
        if self.ignore_current_health_issues:
            current_health_issues = self.controller.get_cluster_status().get_health_issues()
        else:
            current_health_issues = {}

        for index, mon_node in enumerate(mon_nodes):
            if self.interactive:
                ask_confirmation(f"Ready to restart the mon daemons for node {mon_node}?")

            LOGGER.info("Restarting mons from node %s, %d done, %d to go", mon_node, index, len(mon_nodes) - index)
            remote_node = self.spicerack.remote().query(
                f"D{{{mon_node}.{self.controller.get_nodes_domain()}}}", use_sudo=True
            )
            run_one_raw(command=["systemctl", "restart", "ceph-mon@*"], node=remote_node)

            LOGGER.info(
                "Restarted mon daemons on node %s, %d done, %d to go, waiting for cluster to stabilize...",
                mon_node,
                index + 1,
                len(mon_nodes) - index - 1,
            )
            try:
                self.controller.wait_for_cluster_healthy(
                    health_issues_to_ignore=current_health_issues.keys(),
                    consider_maintenance_healthy=True,
                )
                LOGGER.info("Cluster stable, continuing")
            except CephClusterUnhealthy:
                if self.force:
                    LOGGER.warning("Cluster is not stable, but force was passed, continuing...")
                else:
                    raise

        self.controller.unset_maintenance(silences=silences, force=self.force or self.ignore_current_health_issues)
        self.sallogger.log(message=f"Finished restarting all the mon daemons from the nodes {mon_nodes}")
