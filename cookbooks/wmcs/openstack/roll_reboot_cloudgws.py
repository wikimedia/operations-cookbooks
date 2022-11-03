r"""WMCS Openstack - Rolling reboot of all the cloudgw.

Usage example:
    cookbook wmcs.openstack.roll_reboot_cloudgws --cluster_name eqiad1

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import get_gateway_nodes
from cookbooks.wmcs.openstack.cloudgw.reboot_node import RebootNode
from cookbooks.wmcs.openstack.network.tests import NetworkTests

LOGGER = logging.getLogger(__name__)


class RollRebootCloudgws(CookbookBase):
    """WMCS Openstack cookbook to rolling reboot all cloudgws."""

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
            choices=list(OpenstackClusterName),
            type=OpenstackClusterName,
            help="Cluster/deployment to roll-reboot the cloudgws for.",
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
        return with_common_opts(self.spicerack, args, RollRebootCloudgwsRunner,)(
            force=args.force,
            cluster_name=args.cluster_name,
            spicerack=self.spicerack,
        )


def check_network_ok(cluster_name: OpenstackClusterName, spicerack: Spicerack) -> None:
    """Run the network tests and check if they pass."""
    args = ["--cluster_name", str(cluster_name)]
    network_test_cookbook = NetworkTests(spicerack=spicerack)
    if network_test_cookbook.get_runner(args=network_test_cookbook.argument_parser().parse_args(args)).run() != 0:
        raise Exception("Network tests failed, see logs or run the cookbook for details.")


class RollRebootCloudgwsRunner(WMCSCookbookRunnerBase):
    """Runner for RollRebootCloudgws"""

    def __init__(
        self,
        common_opts: CommonOpts,
        force: bool,
        cluster_name: OpenstackClusterName,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.force = force
        super().__init__(spicerack=spicerack)
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.cluster_name = cluster_name
        self.cloudgw_hosts = get_gateway_nodes(cluster_name=cluster_name)
        if not self.force:
            LOGGER.info("Checking the current state of the network...")
            check_network_ok(cluster_name=self.cluster_name, spicerack=self.spicerack)
            LOGGER.info("Network up and running!")

    def run_with_proxy(self) -> None:
        """Main entry point"""
        self.sallogger.log(
            message=(
                f"Rebooting all the cloudgw nodes from the {self.cluster_name} cluster_name: "
                + ",".join(self.cloudgw_hosts)
            )
        )

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, cloudgw_node in enumerate(self.cloudgw_hosts):
            LOGGER.info("Rebooting node %s, %d done, %d to go", cloudgw_node, index, len(self.cloudgw_hosts) - index)
            args = [
                "--fqdn-to-reboot",
                f"{cloudgw_node}",
                "--skip-checks",
            ] + self.common_opts.to_cli_args()

            reboot_node_cookbook.get_runner(args=reboot_node_cookbook.argument_parser().parse_args(args)).run()
            LOGGER.info(
                "Rebooted node %s, %d done, %d to go, waiting for cluster to stabilize...",
                cloudgw_node,
                index + 1,
                len(self.cloudgw_hosts) - index - 1,
            )
            if not self.force:
                LOGGER.info("Checking if the network is still up and running...")
                check_network_ok(cluster_name=self.cluster_name, spicerack=self.spicerack)
                LOGGER.info("Network up and running! Will continue.")

        self.sallogger.log(message=f"Finished rebooting the cloudgw nodes {self.cloudgw_hosts}")
