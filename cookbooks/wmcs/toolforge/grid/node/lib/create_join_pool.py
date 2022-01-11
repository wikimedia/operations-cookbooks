r"""WMCS Toolforge - create a new grid node, make it join the grid and pool it

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.create_join_pool \\
        --project toolsbeta
        --nodetype webgen
"""
# pylint: disable=too-many-arguments
import argparse
import datetime
import logging
from typing import Optional
from enum import Enum
from dataclasses import replace

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs import GridController, dologmsg, DebianVersion
from cookbooks.wmcs.toolforge.start_instance_with_prefix import (
    InstanceCreationOpts,
    StartInstanceWithPrefix,
    add_instance_creation_options,
    with_instance_creation_options,
)
from cookbooks.wmcs.vps.refresh_puppet_certs import RefreshPuppetCerts

LOGGER = logging.getLogger(__name__)


class GridNodeType(Enum):
    """Represents a grid node type."""

    WEBGEN = "webgen"


class ToolforgeGridNodeCreateJoinPool(CookbookBase):
    """WMCS Toolforge cookbook to create a new node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument(
            "--project",
            required=True,
            help="Openstack project where the toolforge installation resides.",
        )
        add_instance_creation_options(parser)
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this operation (ex. T123456)",
        )
        parser.add_argument(
            "--grid-master-fqdn",
            required=False,
            default=None,
            help=(
                "FQDN of the grid master, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        parser.add_argument(
            "--debian-version",
            required=True,
            default=DebianVersion.BUSTER.name.lower(),
            choices=[version.name.lower() for version in DebianVersion],
            # TODO: Figure out the debian version from the image, or just not use it for the prefix
            help="Version of debian to use, as currently we are unable to get it from the image reliably.",
        )
        parser.add_argument(
            "--nodetype",
            required=True,
            choices=[ntype.value for ntype in GridNodeType],
            help="Type of the new grid node",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_instance_creation_options(args, ToolforgeGridNodeCreateJoinPoolRunner)(
            project=args.project,
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            debian_version=DebianVersion[args.debian_version.upper()],
            task_id=args.task_id,
            spicerack=self.spicerack,
            nodetype=args.nodetype,
        )


class ToolforgeGridNodeCreateJoinPoolRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeCreateJoinPool"""

    def __init__(
        self,
        project: str,
        grid_master_fqdn: str,
        task_id: str,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
        nodetype: GridNodeType,
        debian_version: DebianVersion = DebianVersion.BUSTER,
    ):
        """Init"""
        self.project = project
        self.grid_master_fqdn = grid_master_fqdn
        self.task_id = task_id
        self.spicerack = spicerack
        self.debian_version = debian_version
        self.instance_creation_opts = instance_creation_opts
        self.nodetype = nodetype

    def run(self) -> Optional[int]:
        """Main entry point"""
        if not self.instance_creation_opts.prefix:
            self.instance_creation_opts = replace(
                self.instance_creation_opts, prefix=f"{self.project}-sge{self.nodetype}-{self.debian_version.value}"
            )

        start_args = [
            "--project",
            self.project,
            "--ssh-retries",
            "60",  # 1H. Installing the exec environment (via puppet) takes a really long time.
        ] + self.instance_creation_opts.to_cli_args()

        start_instance_cookbook = StartInstanceWithPrefix(spicerack=self.spicerack)
        response = start_instance_cookbook.get_runner(
            args=start_instance_cookbook.argument_parser().parse_args(start_args)
        ).run()
        new_member_fqdn = response.server_fqdn
        node = self.spicerack.remote().query(f"D{{{new_member_fqdn}}}", use_sudo=True)

        LOGGER.info("Making sure that the proper puppetmaster is setup for the new node %s", new_member_fqdn)
        LOGGER.info("It might fail before rebooting, will make sure it runs after too.")
        refresh_puppet_certs_cookbook = RefreshPuppetCerts(spicerack=self.spicerack)
        refresh_puppet_certs_cookbook.get_runner(
            args=refresh_puppet_certs_cookbook.argument_parser().parse_args(
                ["--fqdn", new_member_fqdn, "--pre-run-puppet", "--ignore-failures"]
            ),
        ).run()

        LOGGER.info("Rebooting new node %s to make sure everything is well installed.", new_member_fqdn)
        reboot_time = datetime.datetime.utcnow()
        node.reboot()
        node.wait_reboot_since(since=reboot_time)

        LOGGER.info("Rebooted node %s, running puppet again, this time it should work.", new_member_fqdn)
        PuppetHosts(remote_hosts=node).run(timeout=30 * 60)

        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        grid_controller.add_node(host_fqdn=new_member_fqdn, is_tools_project=(self.project == "tools"))

        dologmsg(
            project=self.project,
            message=f"created node {new_member_fqdn} and added it to the grid",
            task_id=self.task_id,
        )
