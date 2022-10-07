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
    cookbook sre.k8s.reboot-nodes --alias wikikube-staging-worker-codfw -g main --batchsize 1 reboot

This command will cause a rolling reboot of the nodes in the Kubernetes-staging
cluster, one at a time per taint-group, waiting 35 seconds before rebooting.
"""
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from math import ceil
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
    # Wait for 5 seconds between batches.
    # This happens after uncordoning. Daemonsets should have already been scheduled and
    # Puppet run plus Icinga checks do take long enough for everything to settle.
    #
    # Leave a 2 sec sleep still as it's a good time for ^C
    grace_sleep = 2
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
        if not args.alias:
            raise RuntimeError("Alias (-a/--alias) is required for this cookbook, --query is not supported.")
        return RollRebootK8sNodesRunner(args, self.spicerack)


class RollRebootK8sNodesRunner(SRELBBatchRunnerBase):
    """Group all nodes of a Kubernetes cluster by taints and perform rolling reboots on a per taint-group basis"""

    depool_threshold = 5  # Maximum allowed batch size
    # Seconds to sleep after the depool.
    # This happens after nodes have been drained
    depool_sleep = 35
    # Seconds to sleep before the repool.
    # As this happens prior to uncordoning, we can rely on grace_sleep and don't wait for repool.
    repool_sleep = 0

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        # Init k8s_client early, as it is used in _hosts() which will be called by super().__init__
        self.k8s_cli = Kubernetes(
            group=args.group,
            cluster=self._kubernetes_cluster_name(args.alias),
            dry_run=spicerack.dry_run,
        )
        # Dictionary containing KubernetesNode instances for all hosts
        self._all_k8s_nodes = dict()
        super().__init__(args, spicerack)
        # _first_batch is used to detect the fist batch run in each host_group
        self._first_batch = True
        # _host_group_idx stores the index of the host group currently in progress
        self._host_group_idx = 0

    def _kubernetes_cluster_name(self, alias: str) -> str:
        """Return the name of the kubernetes cluster used for credentials files (in /etc/kubernetes)

        Unfortunately, clusters are named differently in cumin aliases/conftool and hiera kubernetes_cluster_groups
        and the cluster group (main/wikikube and ml-serve) is not part of the kubernetes credential files.
        Also, transition from group name "main" to "wikikube" is not completed.
        """
        datacenter = alias.rsplit("-", 1)[1]
        if alias.startswith("wikikube-worker"):
            return datacenter
        if alias.startswith("wikikube-staging-worker"):
            return f"staging-{datacenter}"
        if alias.startswith("ml-serve"):
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

    def _batchsize(self, number_of_hosts: int) -> int:
        """Adjust the batch size to be no more than 20% of the host in each node/taint group"""
        orig_batchsize = super()._batchsize(number_of_hosts)
        batchsize = ceil(min(20 * number_of_hosts / 100, orig_batchsize))
        if batchsize != orig_batchsize:
            self.logger.warn(
                "Using reduced batchsize of %s due to small host group (%s hosts)",
                batchsize,
                number_of_hosts,
            )
        return batchsize

    @property
    def allowed_aliases(self) -> List:
        """Return a list of allowed aliases for this cookbook"""
        # The single dse-k8s-worker alias does not have corresponding aliases in CORE_DATACENTERS
        aliases = ["dse-k8s-worker"]
        for alias in ["wikikube-worker", "wikikube-staging-worker", "ml-serve-worker"]:
            aliases.extend(f"{alias}-{dc}" for dc in CORE_DATACENTERS)
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        # The following query must include all hosts matching all the allowed_aliases
        return 'A:wikikube-worker or A:wikikube-staging-worker or A:ml-serve-worker or A:dse-k8s-worker'

    def _hosts(self) -> List[RemoteHosts]:
        all_hosts = super()._hosts()[0]

        # All host names grouped by their taints
        taint_groups = defaultdict(list)
        for node_name in all_hosts.hosts:
            try:
                k8s_node = self.k8s_cli.get_node(node_name)
                # Error out if a node is cordoned as the cookbook would unconditionally uncordon it later
                if not k8s_node.is_schedulable():
                    raise RuntimeError(
                        f"Node {node_name} is cordoned. Only run this cookbook with all nodes uncordoned."
                    )
                self._all_k8s_nodes[node_name] = k8s_node
                flat_taints = (
                    ""
                    if k8s_node.taints is []
                    else flatten_taints(k8s_node.taints)
                )
            except KubernetesApiError:
                # This node is not registered in kubernetes API.
                # Create a dedicated taint group for those as we probably
                # want to reboot them anyways.
                flat_taints = "HasNotJoinedK8sCluster"

            taint_groups[flat_taints].append(node_name)

        self.logger.info(
            "Got %s nodes in %s taint-group(s) %s",
            len(all_hosts),
            len(taint_groups),
            [len(g) for g in taint_groups.values()],
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
        # The node(s) will be drained prior to being depooled. Not ideal but okay for now.
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
            self.logger.info(
                "Cordoning remaining hosts in host group: %s", remaining_hosts
            )
            for node_name in remaining_hosts:
                self._cordon(node_name)
