r"""WMCS Toolforge - grid - join existing grid exec/web node to the cluster

Usage example:
    cookbook wmcs.toolforge.grid.node.lib.join \\
        --project toolsbeta \\
        --nodes-query toolsbeta-sgewebgen-09-2,toolsbeta-sgeweblight-10-[10-12]
"""
import argparse
import logging
from typing import Optional

from ClusterShell.NodeSet import NodeSetParseError
from cumin.backends import InvalidQueryError
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts
from spicerack.remote import RemoteError

from cookbooks.wmcs import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.grid import GridController
from cookbooks.wmcs.libs.openstack import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class ToolforgeGridNodeJoin(CookbookBase):
    """WMCS Toolforge cookbook to join a grid node in the cluster"""

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
            "--grid-master-fqdn",
            required=False,
            default=None,
            help=(
                "FQDN of the grid master, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )
        parser.add_argument(
            "--nodes-query",
            required=True,
            help="hostname node query for the cumin backend.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, it will try to add the nodes even if they are half set up.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeGridNodeJoinRunner,)(
            grid_master_fqdn=args.grid_master_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            nodes_query=args.nodes_query,
            force=args.force,
            spicerack=self.spicerack,
        )


class ToolforgeGridNodeJoinRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridNodeJoin."""

    def __init__(
        self,
        common_opts: CommonOpts,
        nodes_query: str,
        grid_master_fqdn: str,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.grid_master_fqdn = grid_master_fqdn
        self.spicerack = spicerack
        self.nodes_query = nodes_query
        self.force = force
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def _run(self, new_node_fqdn: str):
        # a puppet run is required to make sure grid config files are generated
        LOGGER.info(
            "INFO: running puppet before adding node %s to the grid in %s", new_node_fqdn, self.common_opts.project
        )
        node = self.spicerack.remote().query(f"D{{{new_node_fqdn}}}", use_sudo=True)
        PuppetHosts(remote_hosts=node).run(timeout=30 * 60)

        LOGGER.info("INFO: adding the node %s to the grid in %s", new_node_fqdn, self.common_opts.project)
        grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.grid_master_fqdn)
        grid_controller.add_node(
            host_fqdn=new_node_fqdn, is_tools_project=(self.common_opts.project == "tools"), force=self.force
        )

    def run(self) -> Optional[int]:
        """Main entry point"""
        try:
            remote_hosts = self.spicerack.remote().query(f"D{{{self.nodes_query}}}")
            requested_nodes = remote_hosts.hosts
        except InvalidQueryError as exc:
            LOGGER.error("ERROR: invalid query: %s", exc)
            return 1
        except NodeSetParseError as exc:
            LOGGER.error("ERROR: unable to parse nodeset syntax: %s", exc)
            return 1
        except RemoteError as exc:
            LOGGER.error("ERROR: the cumin query failed: %s", exc.__context__)
            return 1

        openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(),
            control_node_fqdn="cloudcontrol1005.wikimedia.org",
            project=self.common_opts.project,
        )

        actual_nodes = openstack_api.server_list_filter_exists(
            requested_nodes[:], print_output=False, print_progress_bars=False
        )

        for node in set(requested_nodes) - set(actual_nodes):
            LOGGER.warning("WARNING: node %s is not a VM in project %s, ignoring", node, self.common_opts.project)

        if not actual_nodes:
            # not an error because if the nodes are already joined, then a NOOP is expected anyway
            LOGGER.warning("WARNING: no nodes to operate on")
            return 0

        for hostname in actual_nodes:
            self.sallogger.log(
                message=f"trying to join node {hostname} to the grid cluster in {self.common_opts.project}.",
            )
            self._run(f"{hostname}.{self.common_opts.project}.eqiad1.wikimedia.cloud")
        return 0
