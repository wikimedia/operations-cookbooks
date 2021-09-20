"""WMCS Toolforge - Drain a k8s worker node

Usage example:
    cookbook wmcs.toolforge.worker.drain \
        --control-node-fqdn toolsbeta-test-control-5.toolsbeta.eqiad1.wikimedia.cloud \
        --hostname-to-drain toolsbeta-test-worker-4
"""
import argparse
import json
import logging
import time
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import K8S_SYSTEM_NAMESPACES, KubernetesController, dologmsg

LOGGER = logging.getLogger(__name__)


class Drain(CookbookBase):
    """WMCS Toolforge cookbook to drain a k8s worker node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--project",
            required=True,
            help="Toolforge project name, mainly for logging (ex. toolsbeta).",
        )
        parser.add_argument(
            "--control-node-fqdn",
            required=True,
            help="FQDN of a control node in the cluster.",
        )
        parser.add_argument(
            "--hostname-to-drain",
            required=True,
            help="Hostname (without domain) of the node to drain.",
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this operation (ex. T123456)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return DrainRunner(
            hostname_to_drain=args.hostname_to_drain,
            control_node_fqdn=args.control_node_fqdn,
            project=args.project,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class DrainRunner(CookbookRunnerBase):
    """Runner for Drain"""

    def __init__(
        self,
        hostname_to_drain: str,
        control_node_fqdn: str,
        project: str,
        task_id: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.control_node_fqdn = control_node_fqdn
        self.hostname_to_drain = hostname_to_drain
        self.spicerack = spicerack
        self.project = project
        self.task_id = task_id

    def run(self) -> Optional[int]:
        """Main entry point"""
        remote = self.spicerack.remote()
        dologmsg(message=f"Draining node {self.hostname_to_drain}...", project=self.project, task_id=self.task_id)
        kubectl = KubernetesController(remote=remote, controlling_node_fqdn=self.control_node_fqdn)
        kubectl.drain_node(node_hostname=self.hostname_to_drain)

        def _get_non_system_pods():
            pods = kubectl.get_pods_for_node(node_hostname=self.hostname_to_drain)
            return [pod for pod in pods if pod["metadata"]["namespace"] in K8S_SYSTEM_NAMESPACES]

        tries = 0
        max_tries = 10
        while True:
            non_system_pods = _get_non_system_pods()
            if not non_system_pods:
                break

            tries += 1
            if tries > max_tries:
                raise Exception(
                    f"Unable to drain node {self.hostname_to_drain}, still has {len(non_system_pods)} pods running, "
                    f"please check manually. Running pods:\n{json.dumps(non_system_pods, indent=4)}"
                )

            LOGGER.debug(
                "Waiting for node %s to stop all it's pods, still %d running ...",
                self.hostname_to_drain,
                len(non_system_pods),
            )
            time.sleep(30)

        dologmsg(message=f"Drained node {self.hostname_to_drain}.", project=self.project, task_id=self.task_id)
