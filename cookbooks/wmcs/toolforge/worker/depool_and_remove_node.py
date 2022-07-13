"""WMCS Toolforge - Depool and delete the given k8s worker node from a toolforge installation

Usage example:
    cookbook wmcs.toolforge.worker.depool_and_remove_node \
        --project toolsbeta \
        --control-node-fqdn toolsbeta-test-control-5.toolsbeta.eqiad1.wikimedia.cloud \
        --hostname-to-drain toolsbeta-test-worker-4

"""
import argparse
import logging
from typing import Any, Dict, List, Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import (
    CommonOpts,
    KubernetesController,
    SALLogger,
    add_common_opts,
    natural_sort_key,
    with_common_opts,
)
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI
from cookbooks.wmcs.toolforge.worker.drain import Drain
from cookbooks.wmcs.vps.remove_instance import RemoveInstance

LOGGER = logging.getLogger(__name__)


class ToolforgeDepoolAndRemoveNode(CookbookBase):
    """WMCS Toolforge cookbook to remove and delete an existing k8s worker node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--fqdn-to-remove",
            required=False,
            default=None,
            help="FQDN of the node to remove, if none passed will remove the instance with the lower index.",
        )
        parser.add_argument(
            "--control-node-fqdn",
            required=False,
            default=None,
            help="FQDN of the k8s control node, if none passed will try to get one from openstack.",
        )
        parser.add_argument(
            "--k8s-worker-prefix",
            required=False,
            default=None,
            help=("Prefix for the k8s worker nodes, default is <project>-k8s-worker"),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, ToolforgeDepoolAndRemoveNodeRunner,)(
            k8s_worker_prefix=args.k8s_worker_prefix,
            fqdn_to_remove=args.fqdn_to_remove,
            control_node_fqdn=args.control_node_fqdn,
            spicerack=self.spicerack,
        )


class ToolforgeDepoolAndRemoveNodeRunner(CookbookRunnerBase):
    """Runner for ToolforgeDepoolAndRemoveNode"""

    def __init__(
        self,
        common_opts: CommonOpts,
        k8s_worker_prefix: str,
        control_node_fqdn: str,
        fqdn_to_remove: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.k8s_worker_prefix = k8s_worker_prefix
        self.fqdn_to_remove = fqdn_to_remove
        self.control_node_fqdn = control_node_fqdn
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn="cloudcontrol1003.wikimedia.org",
            project=self.common_opts.project,
        )
        self._all_project_servers: Optional[List[Dict[str, Any]]] = None
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def _get_oldest_worker(self, k8s_worker_prefix: str) -> str:
        if not self._all_project_servers:
            self._all_project_servers = self.openstack_api.server_list()

        prefix_members = list(
            sorted(
                (
                    server
                    for server in self._all_project_servers
                    if server.get("Name", "noname").startswith(k8s_worker_prefix)
                ),
                key=lambda server: natural_sort_key(server.get("Name", "noname-0")),
            )
        )
        if not prefix_members:
            raise Exception(
                f"No servers in project {self.common_opts.project} with prefix {k8s_worker_prefix}, nothing to remove."
            )

        # TODO: find a way to not hardcode the domain
        return f"{prefix_members[0]['Name']}.{self.common_opts.project}.eqiad1.wikimedia.cloud"

    def _pick_a_control_node(self, k8s_worker_prefix: str) -> str:
        if not self._all_project_servers:
            self._all_project_servers = self.openstack_api.server_list()

        guessed_control_prefix = k8s_worker_prefix.rsplit("-", 1)[0] + "-control"

        prefix_members = list(
            sorted(
                (
                    server
                    for server in self._all_project_servers
                    if server.get("Name", "noname").startswith(guessed_control_prefix)
                ),
                key=lambda server: natural_sort_key(server.get("Name", "noname-0")),
            )
        )

        if not prefix_members:
            raise Exception(
                f"Unable to guess a control node (looking for prefix {guessed_control_prefix}). Make sure that the "
                "given worker prefix is correct or pass explicitly a control node."
            )

        return f"{prefix_members[0]['Name']}.{self.common_opts.project}.eqiad1.wikimedia.cloud"

    def run(self) -> None:
        """Main entry point"""
        self.sallogger.log(
            message=f"Depooling and removing worker {self.fqdn_to_remove or ', will pick the oldest'}.",
        )
        remote = self.spicerack.remote()
        k8s_worker_prefix = (
            self.k8s_worker_prefix if self.k8s_worker_prefix is not None else f"{self.common_opts.project}-k8s-etcd"
        )
        if not self.fqdn_to_remove:
            fqdn_to_remove = self._get_oldest_worker(k8s_worker_prefix=k8s_worker_prefix)
            LOGGER.info("Picked node %s to remove.", fqdn_to_remove)

        else:
            fqdn_to_remove = self.fqdn_to_remove

        if not self.control_node_fqdn:
            control_node_fqdn = self._pick_a_control_node(k8s_worker_prefix=k8s_worker_prefix)
        else:
            control_node_fqdn = self.control_node_fqdn

        drain_cookbook = Drain(spicerack=self.spicerack)
        drain_args = [
            "--hostname-to-drain",
            fqdn_to_remove.split(".", 1)[0],
            "--control-node-fqdn",
            control_node_fqdn,
        ] + self.common_opts.to_cli_args()

        drain_cookbook.get_runner(args=drain_cookbook.argument_parser().parse_args(args=drain_args)).run()

        kubectl = KubernetesController(remote=remote, controlling_node_fqdn=control_node_fqdn)
        kubectl.delete_node(fqdn_to_remove.split(".", 1)[0])

        LOGGER.info("Removing k8s worker member %s...", fqdn_to_remove)
        remove_instance_cookbook = RemoveInstance(spicerack=self.spicerack)
        remove_instance_cookbook.get_runner(
            args=remove_instance_cookbook.argument_parser().parse_args(
                [
                    "--server-name",
                    fqdn_to_remove.split(".", 1)[0],
                    "--no-dologmsg",  # not interested in the inner SAL entry
                ]
                + self.common_opts.to_cli_args(),
            ),
        ).run()

        self.sallogger.log(message=f"Depooled and removed worker {fqdn_to_remove}.")
