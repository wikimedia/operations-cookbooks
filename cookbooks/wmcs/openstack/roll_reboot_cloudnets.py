"""WMCS Openstack - Rolling reboot of all the cloudnet.

Usage example:
    cookbook wmcs.openstack.roll_reboot_cloudnets \
        --deployment eqiad1

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import Deployment, OpenstackAPI, get_control_nodes
from cookbooks.wmcs.libs.openstack.neutron import NeutronController
from cookbooks.wmcs.openstack.cloudnet.reboot_node import RebootNode

LOGGER = logging.getLogger(__name__)


class RollRebootCloudnets(CookbookBase):
    """WMCS Openstack cookbook to rolling reboot all cloudnets."""

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
            "--deployment",
            required=True,
            choices=list(Deployment),
            type=Deployment,
            help="Openstack deployment to roll reboot the cloudnets for.",
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
        return with_common_opts(self.spicerack, args, RollRebootCloudnetsRunner,)(
            deployment=args.deployment,
            force=args.force,
            spicerack=self.spicerack,
        )


class RollRebootCloudnetsRunner(CookbookRunnerBase):
    """Runner for RollRebootCloudnets"""

    def __init__(
        self,
        common_opts: CommonOpts,
        deployment: Deployment,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.controlling_node_fqdn = get_control_nodes(deployment=deployment)[0]
        self.force = force
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn=self.controlling_node_fqdn,
            project=self.common_opts.project,
        )
        self.neutron_controller = NeutronController(openstack_api=self.openstack_api)
        self.cloudnet_hosts = self.neutron_controller.get_cloudnets()
        # Make sure that the primary node is the last to reboot, so we only have one network interruption
        primary_node = self.neutron_controller.get_l3_primary()
        if primary_node not in self.cloudnet_hosts:
            raise Exception(
                "Something weird is happening, the primary node ({primary_node}) for the l3 routers is not in the "
                f"cloudnet list ({self.cloudnet_hosts})"
            )
        self.cloudnet_hosts.pop(self.cloudnet_hosts.index(primary_node))
        self.cloudnet_hosts.append(primary_node)

    def run(self) -> None:
        """Main entry point"""
        self.sallogger.log(message=f"Rebooting all the cloudnet nodes {','.join(self.cloudnet_hosts)}")

        reboot_node_cookbook = RebootNode(spicerack=self.spicerack)
        for index, cloudnet_node in enumerate(self.cloudnet_hosts):
            LOGGER.info("Rebooting node %s, %d done, %d to go", cloudnet_node, index, len(self.cloudnet_hosts) - index)
            args = [
                "--fqdn-to-reboot",
                f"{cloudnet_node}.{self.openstack_api.get_nodes_domain()}",
            ] + self.common_opts.to_cli_args()

            if self.force:
                args.append("--force")

            reboot_node_cookbook.get_runner(args=reboot_node_cookbook.argument_parser().parse_args(args)).run()
            LOGGER.info(
                "Rebooted node %s, %d done, %d to go, waiting for cluster to stabilize...",
                cloudnet_node,
                index + 1,
                len(self.cloudnet_hosts) - index - 1,
            )
            if not self.force:
                self.neutron_controller.wait_for_network_alive()
                LOGGER.info("Neutron cluster stable, continuing")
            else:
                LOGGER.warning("Skipping health checks as --force passed, continuing...")

        self.sallogger.log(message=f"Finished rebooting the cloudnet nodes {self.cloudnet_hosts}")
