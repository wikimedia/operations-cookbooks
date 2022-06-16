"""Reboot all nodes in a Kubernetes cluster.

The cookbook will split all nodes of a cluster into groups by taints and
then process each taint group in the following way:

For batches of nodes in the Kubernetes cluster it will:
- Cordon all hosts
- Drain all hosts
- Set Icinga/Alertmanager downtime for all hosts in the batch to reboot
- Reboot
- Wait for hosts to come back online
- If reboot: Wait for the first puppet run
- Wait for Icinga optimal status
- Uncordon all hosts
- Remove the Icinga/Alertmanager downtime

After the first batch has been processed, it will try to avoid re-scheduling of
Pod's as far as possible by cordoning all hosts that are still to be rebooted
(scheduling of drained Pod's will only happen onto hosts that have already been
rebooted).

Usage example:
    cookbook sre.k8s.reboot-nodes -D eqiad -c kubernetes-staging --batchsize 2

This command will cause a rolling reboot of the nodes in the Kubernetes-staging
cluster, 5% at a time per taint-group, waiting 45 seconds before rebooting.
"""
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from typing import List

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase
from kubernetes.client.models import V1Taint
from spicerack import Spicerack
from spicerack.constants import CORE_DATACENTERS
from spicerack.k8s import Kubernetes, KubernetesApiError
from spicerack.remote import RemoteHosts


def flatten_taints(taints: List[V1Taint]) -> str:
    """Flatten a taints structure (as returned by Kubernetes API) into a string

    This is used to group nodes by taints, making sure the order in which they are
    returned by the API does not matter.
    """
    return ";".join(
        [f"{t.key}={t.value}:{t.effect}" for t in sorted(taints, key=lambda a: a.key)]
    )


class RollRebootK8sNodes(SREBatchBase):
    """Kubernetes cluster nodes reboot"""

    batch_default = 1
    batch_max = 5
    grace_sleep = 10
    valid_actions = ("reboot",)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()

        parser.add_argument(
            "--group",
            "-g",
            help="Cluster group (as in hiera kubernetes_cluster_groups) of the cluster to restart",
        )

        return parser

    def get_runner(self, args: Namespace) -> "RollRebootK8sNodesRunner":
        """As specified by Spicerack API."""
        return RollRebootK8sNodesRunner(args, self.spicerack)


class RollRebootK8sNodesRunner(SRELBBatchRunnerBase):
    """Group all nodes of a Kubernetes cluster by taints and perform rolling reboots on a per taint-group basis"""

    depool_threshold = 5  # Maximum allowed batch size
    depool_sleep = 35  # Seconds to sleep after the depool before the restart
    repool_sleep = 5  # Seconds to sleep before the repool after the restart

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        super().__init__(args, spicerack)
        self.k8s_cli = Kubernetes(
            self._args.group,
            self._kubernetes_cluster_name(),
            self._spicerack.dry_run,
        )
        # Dictionary containing KubernetesNode instances for all hosts
        self._all_k8s_nodes = dict()
        # _first_batch is used to detect the fist batch run in each host_group
        self._first_batch = True
        # _host_group_idx stores the index of the host group currently in progress
        self._host_group_idx = 0

    def _kubernetes_cluster_name(self) -> str:
        """Return the name of the kubernetes cluster used for credentials files (in /etc/kubernetes)

        Unfortunately, clusters are named differently in cumin aliases/conftool and hiera kubernetes_cluster_groups
        and the cluster group (main/wikikube and ml-serve) is not part of the kubernetes credential files.
        Also, transition from group name "main" to "wikikube" is not completed.
        """
        datacenter = self._args.alias.rsplit("-", 1)[1]
        if self._args.alias.startswith("wikikube-worker"):
            return datacenter
        if self._args.alias.startswith("wikikube-staging-worker"):
            return f"staging-{datacenter}"
        if self._args.alias.startswith("ml-serve"):
            return f"ml-serve-{datacenter}"

    def _k8s_node_action(self, node_name: str, action: str) -> None:
        """Call the function action on a KubernetesNode instance for a given node_name"""
        node = self._all_k8s_nodes.get(node_name)
        return None if node is None else getattr(node, action)()

    def _cordon(self, node_name: str) -> None:
        """Cordon a kubernetes node"""
        return self._k8s_node_action(node_name, "cordon")

    def _uncordon(self, node_name: str) -> None:
        """Uncordon a kubernetes node"""
        return self._k8s_node_action(node_name, "uncordon")

    def _drain(self, node_name: str) -> None:
        """Drain a kubernetes node"""
        return self._k8s_node_action(node_name, "drain")

    def allowed_aliases(self) -> List:
        """Return a list of allowed aliases for this cookbook"""
        aliases = []
        for alias in ["wikikube-worker", "wikikube-staging-worker", "ml-serve-worker"]:
            aliases.extend(f"{alias}-{dc}" for dc in CORE_DATACENTERS)
        return aliases

    def _hosts(self) -> List[RemoteHosts]:
        all_hosts = super()._hosts()[0]

        # All host names grouped by their taints
        taint_groups = defaultdict(list)
        for node_name in all_hosts.hosts:
            try:
                k8s_node = self.k8s_cli.get_node(node_name)
                self._all_k8s_nodes[node_name] = k8s_node
                flat_taints = (
                    ""
                    if k8s_node._node.spec.taints is None
                    else flatten_taints(k8s_node._node.spec.taints)
                )
            except KubernetesApiError:
                # This node is not registered in kubernetes API.
                # Create a dedicated taint group for those as we probably
                # want to reboot them anyways.
                flat_taints = "HasNotJoinedK8sCluster"

            taint_groups[flat_taints].append(node_name)

        self.logger.info(
            "Got %s nodes in %s taint-groups",
            len(all_hosts),
            len(taint_groups),
        )
        # Build a list of RemoteHosts instances to return
        hosts = []
        for host_names in taint_groups.values():
            hosts.append(self._spicerack.remote().query(",".join(host_names)))
        return hosts

    def group_action(self, host_group_idx, _: int) -> None:
        """Action to perform once for every host group, right before working on the first batch

        Arguments:
            host_group_idx (`int`): the index of the current host group in self.host_groups
            number_of_batches (`int`): the total number of batches in this host group

        """
        self._host_group_idx = host_group_idx
        self._first_batch = True

    def pre_action(self, batch: RemoteHosts) -> None:
        """Cordon all nodes in this batch first, then drain them

        Cordoning first is to prevent evicted Pods from being scheduled on nodes
        that are to be rebooted in this batch.
        """
        for node_name in batch.hosts:
            self._cordon(node_name)
        for node_name in batch.hosts:
            self._drain(node_name)

    def post_action(self, batch: RemoteHosts) -> None:
        """Uncordon all node in this batch and cordon all nodes in this taint group that still need reboots

        Cordoning all remaining (to be rebooted) nodes of this taint group prevents evicted Pods to be
        scheduled there (and evicted again).
        """
        for node_name in batch.hosts:
            self._uncordon(node_name)

        # If this was the first batch in the host group, cordon all nodes that still need rebooting
        # to prevent evicted Pod's from being scheduled there.
        if self._first_batch:
            self._first_batch = False
            remaining_hosts = self.host_groups[self._host_group_idx].hosts - batch.hosts
            for node_name in remaining_hosts.hosts:
                self._cordon(node_name)