"""Change pooled status of a node in a Kubernetes cluster"""

import logging
import re
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import Optional

from cumin import NodeSet
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.k8s import Kubernetes
from spicerack.remote import RemoteError, RemoteHosts
from wmflib import phabricator
from wmflib.decorators import retry

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES

logger = logging.getLogger(__name__)


class PoolDepoolK8sNodes(CookbookBase):
    """Change pooled status of nodes in a Kubernetes cluster

    Workflow:
    - Set the nodes pool status to inactive
    - Cordon all nodes (to prevent evicted pods from being scheduled on other to be depooled nodes)
    - For each node (one by one):
      - Drain node
    Or:
    - Uncordon the nodes
    - Set their pooled status to yes

    Usage example:
        cookbook sre.k8s.pool-depool-node --k8s-cluster wikikube-codfw pool wikikube-worker200[1-5].codfw.wmnet
        cookbook sre.k8s.pool-depool-node --k8s-cluster wikikube-codfw depool wikikube-worker200[1-5].codfw.wmnet
    """

    argument_reason_required = False
    argument_task_required = False

    def get_runner(self, args: Namespace) -> "PoolDepoolK8sNodesRunner":
        """As specified by Spicerack API."""
        return PoolDepoolK8sNodesRunner(args, self.spicerack)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--k8s-cluster",
            required=True,
            help="K8s cluster the nodes are part of",
            choices=ALLOWED_CUMIN_ALIASES.keys(),
        )
        actions = parser.add_subparsers(dest="action", help="The action to perform")
        action_pool = actions.add_parser("pool")
        action_depool = actions.add_parser("depool")
        action_check = actions.add_parser("check")
        for action in (action_pool, action_depool, action_check):
            action.add_argument(
                "hosts",
                help="Hosts to be pooled/depooled/checked (specified in Cumin query syntax)",
            )

        return parser


class PoolDepoolK8sNodesRunner(CookbookRunnerBase):
    """Drain and depool a single host."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Drain a single host and depool it"""
        self.args = args
        self.spicerack = spicerack
        self.phabricator: Optional[phabricator.Phabricator] = None
        self.k8s_cluster = args.k8s_cluster
        self.actions = self.spicerack.actions
        self.reason = self.spicerack.admin_reason(
            f"{args.action} {args.hosts}" if not args.reason else args.reason
        )

        try:
            self.remote_hosts: RemoteHosts = spicerack.remote().query(
                f"D{{{args.hosts}}} and (A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['workers']} "
                f"or A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['control-plane']})"
            )
        except RemoteError as exc:
            raise RuntimeError(
                f"Cannot find the hosts {args.hosts} among any k8s nodes in cluster {self.k8s_cluster}"
            ) from exc

        self.confctl = self.spicerack.confctl("node")
        self.confctl_services = self.confctl.filter_objects(
            {}, name="|".join(self.remote_hosts.hosts.striter())
        )
        if not self.confctl_services:
            raise RuntimeError(
                f"No confctl objects found for hosts {self.remote_hosts.hosts}"
            )
        # Get the expected number of BGP sessions for each host from netbox
        self.expected_bgp_session_counts: dict[str, int] = {}
        for host in self.remote_hosts.hosts:
            self.expected_bgp_session_counts[host] = self._get_expected_bgp_session_count(host)

        self.k8s_cli = Kubernetes(
            group=ALLOWED_CUMIN_ALIASES[self.k8s_cluster]["k8s-group"],
            # The cluster name expected here might be different from the one in the cumin alias
            # wikikube is an example of this as we call it wikikube-eqiad in cumin, but eqiad in k8s config
            cluster=ALLOWED_CUMIN_ALIASES[self.k8s_cluster]["k8s-cluster"],
            dry_run=spicerack.dry_run,
        )

        if args.task_id is not None:
            self.phabricator = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.post_to_phab(
                f"{args.action} host {self.remote_hosts.hosts} by {self.reason.owner} with reason: {args.reason}\n"
            )
        else:
            self.phabricator = None

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return "{} for host {}".format(self.args.action, self.remote_hosts.hosts)

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.remote_hosts.hosts), concurrency=1, ttl=600)

    def post_to_phab(self, message: Optional[str] = None) -> None:
        """Comment on the phabricator task"""
        if self.phabricator is not None:
            if message is None:
                message = (
                    f"Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:\n"
                    f"{self.actions}\n"
                )
            self.phabricator.task_comment(self.args.task_id, message)

    def _get_expected_bgp_session_count(self, host: str) -> int:
        """Check how many BGP sessions are expected for this host (new and old topology)"""
        logger.info("Getting vlan info for %s from netbox", host)
        netbox_server = self.spicerack.netbox_server(
            host.split(".")[0], read_write=False
        )
        if netbox_server.virtual:
            # Ganeti VMs always peer with the core routers like the old VLANs
            return 4

        vlan = netbox_server.access_vlan
        session_count = 0
        # Old-topology vlans only identify the row, not the rack
        # e.g private1-a-eqiad
        if re.match(r"private1-\w-(eqiad|codfw)", vlan):
            logger.info("%s: Old vlan %s, need 4 Established BGP sessions", host, vlan)
            session_count = 4
        # New-topology vlans identify the row and the rack
        # e.g private1-a1-eqiad
        elif re.match(r"private1-\w{2}-(eqiad|codfw)", vlan):
            logger.info("%s: New vlan %s, need 2 Established BGP sessions", host, vlan)
            session_count = 2
        else:
            raise RuntimeError(f"Unknown vlan {vlan} for host {host}")
        return session_count

    def check_calico_node_status(self, remote_hosts: RemoteHosts) -> bool:
        """Check calicoctl node status"""
        results = remote_hosts.run_sync(
            "calicoctl node status",
            is_safe=True,
            print_progress_bars=False,
            print_output=False,
        )
        all_nodes_ok: bool = True
        for nodeset, output in results:
            established_count = 0
            # Just count the number of established sessions
            for line in output.lines():
                if "Established" in line.decode():
                    established_count += 1

            for node in nodeset:
                try:
                    expected_sessions = self.expected_bgp_session_counts[node]
                except KeyError as exc:
                    raise KeyError(
                        f"Unknown expected session count for {node}"
                    ) from exc

                if established_count != expected_sessions:
                    msg = f"{node}: Expected {expected_sessions} established BGP sessions, got {established_count}"
                    logger.warning(msg)
                    all_nodes_ok = False
        return all_nodes_ok

    @retry(  # pylint: disable=no-value-for-parameter
        tries=10,
        delay=timedelta(seconds=5),
        backoff_mode="constant",
        failure_message="calicoctl node status not Established",
        exceptions=(RuntimeError,),
    )
    def wait_for_calico_node_status_ok(self, remote_hosts: RemoteHosts) -> None:
        """Retry until calicoctl node status is Established"""
        if not self.check_calico_node_status(remote_hosts):
            raise RuntimeError("calicoctl node status not Established")

    def _k8s_node_action(self, nodes: NodeSet, action: str) -> None:
        for node in nodes:
            k8s_node = self.k8s_cli.get_node(node)
            action_method = getattr(k8s_node, action)
            action_method()

    def run(self):
        """Uncordon and pool or cordon, drain, and depool the host"""
        if self.args.action == "pool":
            logger.info("Checking calicoctl node status")
            self.wait_for_calico_node_status_ok(self.remote_hosts)
            logger.info("Pooling %s in %s", self.remote_hosts.hosts, self.k8s_cluster)
            self.confctl.update_objects(
                {"pooled": "yes", "weight": 10}, self.confctl_services
            )
            self._k8s_node_action(self.remote_hosts.hosts, "uncordon")
            self.actions[str(self.remote_hosts.hosts)].success(
                f"Host {self.remote_hosts.hosts} pooled in {self.k8s_cluster}"
            )
            logger.info("%s completed:\n%s\n", __name__, self.actions)
            self.post_to_phab()
        elif self.args.action == "depool":
            logger.info(
                "Depooling %s from %s", self.remote_hosts.hosts, self.k8s_cluster
            )
            self.confctl.update_objects({"pooled": "inactive"}, self.confctl_services)
            self._k8s_node_action(self.remote_hosts.hosts, "cordon")
            logger.info("Draining %s", self.remote_hosts.hosts)
            self._k8s_node_action(self.remote_hosts.hosts, "drain")
            self.actions[str(self.remote_hosts.hosts)].success(
                f"Host {self.remote_hosts.hosts} depooled from {self.k8s_cluster}"
            )
            logger.info("%s completed:\n%s\n", __name__, self.actions)
            self.post_to_phab()
        elif self.args.action == "check":
            for service in self.confctl_services:
                logger.info(
                    "%s confctl status: %s=%s",
                    service.name,
                    service.tags["service"],
                    (
                        "pooled"
                        if getattr(service, "pooled") == "yes"
                        else getattr(service, "pooled")
                    ),
                )
            for host in self.remote_hosts.hosts:
                k8s_node = self.k8s_cli.get_node(host)
                logger.info(
                    "%s k8s status: %s",
                    host,
                    ("schedulable" if k8s_node.is_schedulable() else "unschedulable"),
                )
            self.check_calico_node_status(self.remote_hosts)
