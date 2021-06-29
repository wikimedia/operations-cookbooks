"""WMCS Toolforge - Depool and delete the given k8s worker node from a toolforge installation

Usage example:
    cookbook wmcs.toolforge.worker.depool_and_remove_node \
        --project toolsbeta \
        --control-node-fqdn toolsbeta-test-control-5.toolsbeta.eqiad1.wikimedia.cloud \
        --hostname-to-drain toolsbeta-test-worker-4

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import KubernetesController, OpenstackAPI, dologmsg, natural_sort_key
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
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--fqdn-to-remove",
            required=False,
            default=None,
            help="FQDN of the node to remove, if none passed will remove the intance with the lower index.",
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
        return ToolforgeDepoolAndRemoveNodeRunner(
            k8s_worker_prefix=args.k8s_worker_prefix,
            fqdn_to_remove=args.fqdn_to_remove,
            control_node_fqdn=args.control_node_fqdn,
            project=args.project,
            spicerack=self.spicerack,
        )


class ToolforgeDepoolAndRemoveNodeRunner(CookbookRunnerBase):
    """Runner for ToolforgeDepoolAndRemoveNode"""

    def __init__(
        self,
        k8s_worker_prefix: str,
        control_node_fqdn: str,
        fqdn_to_remove: str,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.k8s_worker_prefix = k8s_worker_prefix
        self.fqdn_to_remove = fqdn_to_remove
        self.control_node_fqdn = control_node_fqdn
        self.project = project
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn="cloudcontrol1003.wikimedia.org",
            project=self.project,
        )
        self._all_project_servers = None

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
            raise Exception(f"No servers in project {self.project} with prefix {k8s_worker_prefix}, nothing to remove.")

        # TODO: find a way to not hardcode the domain
        return f"{prefix_members[0]['Name']}.{self.project}.eqiad1.wikimedia.cloud"

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

        return f"{prefix_members[0]['Name']}.{self.project}.eqiad1.wikimedia.cloud"

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(
            message=f"Depooling and removing worker {self.fqdn_to_remove or ', will pick the oldest'}.",
            project=self.project,
        )
        remote = self.spicerack.remote()
        k8s_worker_prefix = self.k8s_worker_prefix if self.k8s_worker_prefix is not None else f"{self.project}-k8s-etcd"
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
        drain_cookbook.get_runner(
            args=drain_cookbook.argument_parser().parse_args(
                [
                    "--project",
                    self.project,
                    "--hostname-to-drain",
                    fqdn_to_remove.split(".", 1)[0],
                    "--control-node-fqdn",
                    control_node_fqdn,
                ]
            )
        ).run()

        kubectl = KubernetesController(remote=remote, controlling_node_fqdn=control_node_fqdn)
        kubectl.delete_node(fqdn_to_remove.split(".", 1)[0])

        LOGGER.info("Removing k8s worker member %s...", fqdn_to_remove)
        remove_instance_cookbook = RemoveInstance(spicerack=self.spicerack)
        remove_instance_cookbook.get_runner(
            args=remove_instance_cookbook.argument_parser().parse_args(
                [
                    "--project",
                    self.project,
                    "--server-name",
                    fqdn_to_remove.split(".", 1)[0],
                ],
            ),
        ).run()

        dologmsg(
            message=f"Depooled and removed worker {fqdn_to_remove}.",
            project=self.project,
        )
