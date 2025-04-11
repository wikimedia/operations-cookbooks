"""Kubernetes cluster operations."""
import json
import logging
from abc import ABCMeta
from argparse import ArgumentParser, Namespace
from collections import defaultdict
from math import ceil
from typing import Optional, Union

from kubernetes.client.models import V1Taint
from spicerack import Spicerack
from spicerack.cookbook import LockArgs
from spicerack.k8s import KubernetesApiError, KubernetesNode
from spicerack.remote import NodeSet, RemoteExecutionError, RemoteHosts
from wmflib import phabricator

from cookbooks.sre import (PHABRICATOR_BOT_CONFIG_FILE, SREBatchBase,
                           SRELBBatchRunnerBase)

__owner_team__ = "ServiceOps"

logger = logging.getLogger(__name__)

# Prometheus matchers to properly downtime a k8s cluster.
# If we downtime only the hosts we may end up in alerts firing when
# we upgrade, for example due to Calico etc..
PROMETHEUS_MATCHERS: dict[str, list[dict[str, Union[str, int, float, bool]]]] = {
    "staging-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-staging",
            "isRegex": False
        }
    ],
    "staging-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-staging",
            "isRegex": False
        }
    ],
    "wikikube-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s",
            "isRegex": False
        }
    ],
    "wikikube-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s",
            "isRegex": False
        }
    ],
    "ml-serve-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve",
            "isRegex": False
        }
    ],
    "ml-serve-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlserve",
            "isRegex": False
        }
    ],
    "ml-staging-codfw": [
        {
            "name": "site",
            "value": "codfw",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-mlstaging",
            "isRegex": False
        }
    ],
    "dse-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-dse",
            "isRegex": False
        }
    ],
    "aux-eqiad": [
        {
            "name": "site",
            "value": "eqiad",
            "isRegex": False
        },
        {
            "name": "prometheus",
            "value": "k8s-aux",
            "isRegex": False
        }
    ]
}

# Note: The k8s-group field is retrievable in hieradata/kubernetes.yaml
ALLOWED_CUMIN_ALIASES = {
    "staging-codfw": {
        "k8s-group": "main",
        "k8s-cluster": "staging-codfw",
        "etcd": "wikikube-staging-etcd-codfw",
        "control-plane": "wikikube-staging-master-codfw",
        "workers": "wikikube-staging-worker-codfw"
    },
    "staging-eqiad": {
        "k8s-group": "main",
        "k8s-cluster": "staging-eqiad",
        "etcd": "wikikube-staging-etcd-eqiad",
        "control-plane": "wikikube-staging-master-eqiad",
        "workers": "wikikube-staging-worker-eqiad"
    },
    "wikikube-codfw": {
        "k8s-group": "main",
        "k8s-cluster": "codfw",
        "etcd": "wikikube-etcd-codfw",
        "control-plane": "wikikube-master-codfw",
        "workers": "wikikube-worker-codfw"
    },
    "wikikube-eqiad": {
        "k8s-group": "main",
        "k8s-cluster": "eqiad",
        "etcd": "wikikube-etcd-eqiad",
        "control-plane": "wikikube-master-eqiad",
        "workers": "wikikube-worker-eqiad"
    },
    "ml-serve-eqiad": {
        "k8s-group": "ml-serve",
        "k8s-cluster": "ml-serve-eqiad",
        "etcd": "ml-serve-etcd-eqiad",
        "control-plane": "ml-serve-master-eqiad",
        "workers": "ml-serve-worker-eqiad"
    },
    "ml-serve-codfw": {
        "k8s-group": "ml-serve",
        "k8s-cluster": "ml-serve-codfw",
        "etcd": "ml-serve-etcd-codfw",
        "control-plane": "ml-serve-master-codfw",
        "workers": "ml-serve-worker-codfw"
    },
    "ml-staging-codfw": {
        "k8s-group": "ml-serve",
        "k8s-cluster": "ml-staging-codfw",
        "etcd": "ml-staging-etcd",
        "control-plane": "ml-staging-master",
        "workers": "ml-staging-worker"
    },
    "dse-eqiad": {
        "k8s-group": "dse-k8s",
        "k8s-cluster": "dse-k8s-eqiad",
        "etcd": "dse-k8s-etcd",
        "control-plane": "dse-k8s-master",
        "workers": "dse-k8s-worker"
    },
    "aux-eqiad": {
        "k8s-group": "aux-k8s",
        "k8s-cluster": "aux-k8s-eqiad",
        "etcd": "aux-etcd",
        "control-plane": "aux-master",
        "workers": "aux-worker"
    },
}


def flatten_taints(taints: list[V1Taint]) -> str:
    """Flatten a taints structure (as returned by Kubernetes API) into a string

    This is used to group nodes by taints, making sure the order in which they are
    returned by the API does not matter.
    """
    return ";".join(
        [f"{t.key}={t.value}:{t.effect}" for t in sorted(taints, key=lambda a: a.key)]
    )


def etcdctl(command: str) -> str:
    """Prepend the API v3 environment variable and endpoints to an etcdctl command"""
    return f"ETCDCTL_API=3 /usr/bin/etcdctl --endpoints https://$(hostname -f):2379 {command}"


def etcd_cluster_healthy(remote: RemoteHosts) -> bool:
    """Check if the etcd cluster is healthy, logs the status of each member and returns a boolean"""
    cmd = etcdctl("-w json endpoint health --cluster")
    is_healthy = True
    try:
        result = remote.run_sync(
            cmd,
            is_safe=True,
            print_progress_bars=False,
            print_output=False,
        )
    except RemoteExecutionError as exc:
        # etcdctl will return exitcode != 0 if the cluster is unhealthy
        # but JSON output will still be printed, so we try to parse it anyway
        logger.warning(
            "Command '%s' on %s returned exitcode != 0. Error: %s",
            cmd,
            remote.hosts[0],
            exc,
        )
        # We already know the cluster is unhealthy
        is_healthy = False
    _, output = next(result)
    # Stdout and stderr are merged in the output but etcdctl always prints JSON
    # before everything else, so we can just parse the first line.
    cluster_health = json.loads(next(output.lines()))
    logger.info("etcd clusters health:")
    for member in cluster_health:
        state = "healthy" if member["health"] else "unhealthy"
        logger.info("%s, %s", member["endpoint"], state)
        # Consider the cluster not healthy if any member is unhealthy
        if not member["health"]:
            is_healthy = False
    # Consider the cluster not healthy if it has less than 3 members
    if len(cluster_health) < 3:
        logger.error(
            "etcd cluster health check failed. "
            "Expected at least 3 members, got: %s",
            len(cluster_health),
        )
        is_healthy = False
    return is_healthy


class K8sBatchBase(SREBatchBase, metaclass=ABCMeta):
    """Common Kubernetes batch actions CookbookBase class."""

    # Leave a 2 sec sleep still as it's a good time for ^C
    grace_sleep = 2

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()

        parser.add_argument(
            '--k8s-cluster', required=True,
            help='K8s cluster to use as a sanity check and for Cumin aliases.',
            choices=ALLOWED_CUMIN_ALIASES.keys())

        parser.add_argument(
            "--exclude",
            help="List of hosts that should be skipped, in NodeSet notation",
            default="",
        )

        return parser


class K8sBatchRunnerBase(SRELBBatchRunnerBase, metaclass=ABCMeta):
    """k8s version of SRELBBatchRunnerBase. To be used with SREBatchBase."""

    # Seconds to sleep after the depool.
    # This happens after nodes have been drained
    depool_sleep = 35
    # Seconds to sleep before the repool.
    # As this happens prior to uncordoning, we can rely on grace_sleep and don't wait for repool.
    repool_sleep = 0
    # Set pooled=inactive to avoid scap docker_pull failing on the rebooting nodes
    depool_status = "inactive"

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner.

        k8s_metadata: dict with k8s-group, k8s-cluster
        """
        self.phabricator: Optional[phabricator.Phabricator] = None
        # Init k8s_client early, as it is used in _hosts() which will be called by super().__init__
        # The cluster name expected here might be different from the one in the cumin alias
        # wikikube is an example of this as we call it wikikube-eqiad in cumin, but eqiad in k8s config
        self.k8s_metadata = ALLOWED_CUMIN_ALIASES[args.k8s_cluster]
        self.k8s_cli = spicerack.kubernetes(
            group=self.k8s_metadata["k8s-group"],
            cluster=self.k8s_metadata["k8s-cluster"],
        )
        # Dictionary containing KubernetesNode instances for all hosts
        self._all_k8s_nodes: dict[str, KubernetesNode] = {}
        self.exclude = args.exclude
        super().__init__(args, spicerack)

        # _host_group_idx stores the index of the host group currently in progress
        self._host_group_idx = 0
        # _first_batch is used to detect the fist batch run in each host_group
        self._first_batch = True

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.phabricator.task_comment(
                args.task_id,
                (
                    f"Roll-{args.action} of nodes in {args.k8s_cluster} cluster started by {spicerack.username}:\n"
                    + "\n".join([f"* {group}" for group in self.host_groups])
                ),
            )

    @property
    def allowed_aliases(self) -> list:
        """Return a list of allowed aliases for this cookbook"""
        return [self.k8s_metadata["control-plane"], self.k8s_metadata["workers"]]

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook concurrency a small number per cluster."""
        return LockArgs(suffix=self._args.k8s_cluster, concurrency=5, ttl=1800)

    # TODO lock per node and taint group! -- override run()
    # We use Spicerack locking in addition to checking cordoned status in the k8s API, as cordoning
    # can happen fairly late in a potential parallel cookbook run, and it is better UX and
    # sometimes also safer to error out early.

    def _hosts(self) -> list[RemoteHosts]:
        """Groups hosts by taint. Excludes hosts in self.exclude."""
        all_hosts = super()._hosts()[0]
        to_exclude = NodeSet(self.exclude)
        if len(to_exclude) > 0:
            self.logger.info("Excluding %s nodes: %s", len(to_exclude), to_exclude)

        working_hosts = all_hosts.hosts - to_exclude

        # All host names grouped by their taints
        taint_groups = defaultdict(list)
        for node_name in working_hosts:
            try:
                k8s_node = self._get_node_cli(node_name)
                # Error out if a node is cordoned as we can't tell what cordoned it and why
                if not k8s_node.is_schedulable():
                    raise RuntimeError(
                        f"Node {node_name} is cordoned. Only run this cookbook with all nodes uncordoned."
                    )
                flat_taints = flatten_taints(k8s_node.taints)
            except KubernetesApiError:
                # This node is not registered in kubernetes API.
                # Create a dedicated taint group for those as we probably want to operate on them anyways.
                flat_taints = "HasNotJoinedK8sCluster"

            taint_groups[flat_taints].append(node_name)

        self.logger.info(
            "Got %s nodes in %s taint-group(s) %s",
            len(working_hosts),
            len(taint_groups),
            [len(g) for g in taint_groups.values()],
        )

        # Build a list of RemoteHosts instances to return
        hosts = []
        for host_names in taint_groups.values():
            hosts.append(self._spicerack.remote().query(",".join(host_names)))
        return hosts

    def _get_node_cli(self, nodename):
        """Throws KubernetesApiError when the node isn't known to k8s."""
        # not using setdefault because we want lazy eval
        if nodename not in self._all_k8s_nodes:
            self._all_k8s_nodes[nodename] = self.k8s_cli.get_node(nodename)
        return self._all_k8s_nodes[nodename]

    def _k8s_node_action(self, node_name: str, action: str) -> None:
        """Call the function action on a KubernetesNode instance for a given node_name"""
        node = self._get_node_cli(node_name)
        if node is not None:
            getattr(node, action)()

    def _cordon(self, node_name: str) -> None:
        """Cordon a kubernetes node"""
        self._k8s_node_action(node_name, "cordon")

    def _uncordon(self, node_name: str) -> None:
        """Uncordon a kubernetes node"""
        self._k8s_node_action(node_name, "uncordon")

    def _drain(self, node_name: str) -> None:
        """Drain a kubernetes node"""
        self._k8s_node_action(node_name, "drain")

    def _batchsize(self, number_of_hosts: int) -> int:
        """Adjust the batch size to be no more than 20% of the host in each node/taint group"""
        orig_batchsize = super()._batchsize(number_of_hosts)
        batchsize = ceil(min(20 * number_of_hosts / 100, orig_batchsize))
        if batchsize != orig_batchsize:
            self.logger.warning(
                "Using reduced batchsize of %s due to small host group (%s hosts)",
                batchsize,
                number_of_hosts,
            )
        return batchsize

    def group_action(self, host_group_idx, _: int) -> None:
        """Action to perform once for every host group, right before working on the first batch

        Arguments:
            host_group_idx (`int`): the index of the current host group in self.host_groups
            number_of_batches (`int`): the total number of batches in this host group

        """
        self._host_group_idx = host_group_idx
        self._first_batch = True

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Cordon all nodes in this batch first, then drain them

        Cordoning first is to prevent evicted Pods from being scheduled on nodes
        that are to be rebooted in this batch.
        """
        # The node(s) will be drained prior to being depooled. Not ideal but okay for now.
        for node_name in hosts.hosts:
            self._cordon(node_name)
        for node_name in hosts.hosts:
            self._drain(node_name)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Uncordon all node in this batch and cordon all nodes in this taint group that still need reboots

        Cordoning all remaining (to be processed) nodes of this taint group prevents evicted Pods to be
        scheduled there (and evicted again).
        """
        for node_name in hosts.hosts:
            if self._spicerack.actions[node_name].has_failures:
                self.logger.warning("Leaving host %s cordoned due to reimage failure", node_name)
            else:
                self._uncordon(node_name)

        # If this was the first batch in the host group, cordon all nodes that still need work
        # to prevent evicted Pod's from being scheduled there.
        if self._first_batch:
            self._first_batch = False
            remaining_hosts = self.host_groups[self._host_group_idx].hosts - hosts.hosts
            self.logger.info(
                "Cordoning remaining hosts in host group: %s", remaining_hosts
            )
            for node_name in remaining_hosts:
                self._cordon(node_name)
