r"""WMCS Openstack - Reboot a cloudgw node .

Usage example:
    cookbook wmcs.openstack.cloudgw.reboot_node \
    --fqdn-to-reboot cloudgw1002.eqiad.wmnet

"""
import argparse
import logging
from datetime import datetime

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.alerts import downtime_host, uptime_host
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import get_gateway_nodes, get_node_cluster_name
from cookbooks.wmcs.openstack.network.tests import NetworkTests

LOGGER = logging.getLogger(__name__)


class RebootNode(CookbookBase):
    """WMCS Openstack cookbook to reboot a single cloudgws, handling failover."""

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
            "--skip-checks",
            required=False,
            action="store_true",
            help="If passed, will not test the network before or after rebooting the node.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RebootNodeRunner,)(
            fqdn_to_reboot=args.fqdn_to_reboot,
            skip_checks=args.skip_checks,
            spicerack=self.spicerack,
        )


def check_network_ok(cluster_name: OpenstackClusterName, spicerack: Spicerack) -> None:
    """Run the network tests and check if they pass."""
    args = ["--cluster_name", str(cluster_name)]
    network_test_cookbook = NetworkTests(spicerack=spicerack)
    if network_test_cookbook.get_runner(args=network_test_cookbook.argument_parser().parse_args(args)).run() != 0:
        raise Exception("Network tests failed, see logs or run the cookbook for details.")


class RebootNodeRunner(CookbookRunnerBase):
    """Runner for RebootNode"""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn_to_reboot: str,
        skip_checks: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.fqdn_to_reboot = fqdn_to_reboot
        self.skip_checks = skip_checks
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

        self.cluster_name = get_node_cluster_name(node=self.fqdn_to_reboot)

        known_cloudgws = get_gateway_nodes(self.cluster_name)
        if not known_cloudgws:
            raise Exception(f"No cloudgws found for cluster_name {self.cluster_name} :-S")

        if len(known_cloudgws) == 1 and not self.skip_checks:
            raise Exception(
                f"There's only one gateway node for the cluster_name {self.cluster_name} ({known_cloudgws}), and the "
                "network will go dow if rebooted, pass --skip-checks to ignore."
            )

        if self.fqdn_to_reboot not in known_cloudgws:
            raise Exception(
                f"Host {self.fqdn_to_reboot} is not part of the cloudgw for cluster_name {self.cluster_name}"
            )

        if not self.skip_checks:
            LOGGER.info("Checking the current state of the network...")
            check_network_ok(cluster_name=self.cluster_name, spicerack=self.spicerack)
            LOGGER.info("Network up and running!")

    def run(self) -> None:
        """Main entry point"""
        self.sallogger.log(f"Rebooting cloudgw host {self.fqdn_to_reboot}")
        node = self.spicerack.remote().query(f"D{{{self.fqdn_to_reboot}}}", use_sudo=True)
        host_name = self.fqdn_to_reboot.split(".", 1)[0]
        host_silence_id = downtime_host(
            spicerack=self.spicerack,
            host_name=host_name,
            comment="Rebooting with wmcs.openstack.cloudgw.reboot_node",
            task_id=self.common_opts.task_id,
        )

        reboot_time = datetime.utcnow()
        node.reboot()

        node.wait_reboot_since(since=reboot_time)
        LOGGER.info(
            "Rebooted node %s, waiting for cluster to stabilize...",
            self.fqdn_to_reboot,
        )

        if not self.skip_checks:
            LOGGER.info("Checking if the network is up and running")
            check_network_ok(cluster_name=self.cluster_name, spicerack=self.spicerack)
            LOGGER.info("Network up and running!")

        uptime_host(spicerack=self.spicerack, host_name=host_name, silence_id=host_silence_id)
        LOGGER.info("Silences removed.")

        self.sallogger.log(f"Rebooted cloudgw host {self.fqdn_to_reboot}")