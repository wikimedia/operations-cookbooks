r"""WMCS Toolforge - create a new grid node, make it join the grid and pool it

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.create_join_pool \
        --project toolsbeta \
        --nodetype exec
"""
# pylint: disable=too-many-arguments
import argparse
import datetime
import logging
from dataclasses import replace

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs.libs.common import CommonOpts, DebianVersion, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.grid import GridController, GridNodeType
from cookbooks.wmcs.vps.create_instance_with_prefix import (
    CreateInstanceWithPrefix,
    InstanceCreationOpts,
    add_instance_creation_options,
    with_instance_creation_options,
)
from cookbooks.wmcs.vps.refresh_puppet_certs import RefreshPuppetCerts

LOGGER = logging.getLogger(__name__)


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
        add_common_opts(parser, project_default="toolsbeta")
        add_instance_creation_options(parser)
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
            default=DebianVersion.BUSTER,
            choices=list(DebianVersion),
            type=DebianVersion.from_version_str,
            # TODO: Figure out the debian version from the image, or just not use it for the prefix
            help="Version of debian to use, as currently we are unable to get it from the image reliably.",
        )
        parser.add_argument(
            "--nodetype",
            required=True,
            choices=list(GridNodeType),
            type=GridNodeType,
            help="Type of the new grid node",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(
            self.spicerack, args, with_instance_creation_options(args, ToolforgeGridNodeCreateJoinPoolRunner)
        )(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            debian_version=args.debian_version,
            spicerack=self.spicerack,
            nodetype=args.nodetype,
        )


class ToolforgeGridNodeCreateJoinPoolRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeCreateJoinPool"""

    def __init__(
        self,
        common_opts: CommonOpts,
        grid_master_fqdn: str,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
        nodetype: GridNodeType,
        debian_version: DebianVersion = DebianVersion.BUSTER,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.debian_version = debian_version
        self.instance_creation_opts = instance_creation_opts
        self.nodetype = nodetype
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        if not self.instance_creation_opts.prefix:
            self.instance_creation_opts = replace(
                self.instance_creation_opts,
                prefix=f"{self.common_opts.project}-sge{self.nodetype}-{self.debian_version.value}",
            )

        start_args = (
            [
                "--ssh-retries",
                "60",  # 1H. Installing the exec environment (via puppet) takes a really long time.
            ]
            + self.instance_creation_opts.to_cli_args()
            + self.common_opts.to_cli_args()
        )

        create_instance_cookbook = CreateInstanceWithPrefix(spicerack=self.spicerack)
        response = create_instance_cookbook.get_runner(
            args=create_instance_cookbook.argument_parser().parse_args(start_args)
        ).create_instance()
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
        grid_controller.add_node(host_fqdn=new_member_fqdn, is_tools_project=(self.common_opts.project == "tools"))

        self.sallogger.log(message=f"created node {new_member_fqdn} and added it to the grid")
