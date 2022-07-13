"""WMCS Toolforge - grid - reboots all workers of a specific grid execution queue

Usage example:
    cookbook wmcs.toolforge.grid.reboot_workers \
        --project toolsbeta \
        --master-node-fqdn toolsbeta-sgegrid-master.toolsbeta.eqiad1.wikimedia.cloud \
        --no-dologmsg
"""
import argparse
import logging
from datetime import datetime

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import CommonOpts, DebianVersion, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.grid import GridController, GridNodeType

LOGGER = logging.getLogger(__name__)


class ToolforgeGridRebootWorkers(CookbookBase):
    """Toolforge cookbook to reboot grid exec nodes"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--queue",
            required=True,
            choices=[ntype.value for ntype in GridNodeType],
            help="Only reboot workers in this queue.",
        )
        parser.add_argument(
            "--debian-version",
            required=True,
            choices=[version.name.lower() for version in DebianVersion],
            help="Only reboot workers using this Debian version.",
        )
        parser.add_argument(
            "--master-node-fqdn",
            required=False,
            default=None,
            help=(
                "Name of the grid master node, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeGridRebootWorkersRunner)(
            queue=GridNodeType[args.queue.upper()],
            debian_version=DebianVersion[args.debian_version.upper()],
            master_node_fqdn=args.master_node_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            spicerack=self.spicerack,
        )


class ToolforgeGridRebootWorkersRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridRebootWorkersRunner"""

    def __init__(
        self,
        common_opts: CommonOpts,
        queue: GridNodeType,
        debian_version: DebianVersion,
        master_node_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.queue = queue
        self.debian_version = debian_version
        self.master_node_fqdn = master_node_fqdn
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project,
            task_id=common_opts.task_id,
            dry_run=common_opts.no_dologmsg,
        )

    def run(self) -> None:
        """Main entry point"""
        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.master_node_fqdn)

        self.sallogger.log(message=f"rebooting {self.debian_version.name.lower()} {self.queue.value} grid workers")

        # stretch uses format -xxyy (where x is the debian version and y is the worker number),
        # but buster uses -xx-yy, filter on what's needed to match those reliably
        debian_version_filter = (
            f"-{self.debian_version.value}"
            if self.debian_version == DebianVersion.STRETCH
            else f"-{self.debian_version.value}-"
        )

        nodes = [
            node
            for node in grid_controller.get_nodes_info()
            if self.queue.value in node and debian_version_filter in node
        ]

        for node_fqdn in nodes:
            node_name = node_fqdn.split(".")[0]
            LOGGER.info("Rebooting %s", node_name)

            with grid_controller.with_node_depooled(node_name):
                reboot_time = datetime.utcnow()

                remote_node = self.spicerack.remote().query(f"D{{{node_fqdn}}}", use_sudo=True)

                remote_node.reboot()
                remote_node.wait_reboot_since(reboot_time)

        self.sallogger.log(message=f"rebooted {self.debian_version.name.lower()} {self.queue.value} grid workers")
