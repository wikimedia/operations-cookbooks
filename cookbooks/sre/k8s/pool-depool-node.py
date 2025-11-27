"""Change pooled status of a node in a Kubernetes cluster"""

import logging
import re
from argparse import ArgumentParser, Namespace, ArgumentTypeError
from datetime import timedelta
from typing import Optional

from cumin import NodeSet
from spicerack import Spicerack
from spicerack.cookbook import (
    CookbookBase,
    CookbookRunnerBase,
    LockArgs,
    CookbookInitSuccess,
)
from spicerack.k8s import Kubernetes
from spicerack.remote import RemoteError, RemoteHosts
from wmflib.decorators import retry
from wmflib.interactive import ask_confirmation
from wmflib.constants import CORE_DATACENTERS

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts.downtime import enrich_argument_parser_with_downtime_duration
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES

logger = logging.getLogger(__name__)


def validate_rack_format(rack_string: str) -> str:
    """Validates that the rack argument is one letter followed by one or more digits, e.g., A1, B5, C10.

    This format only applies to our core datacenters.
    """
    if not re.fullmatch(r"[A-Z]\d+", rack_string, re.IGNORECASE):
        raise ArgumentTypeError(
            f"Invalid rack format: '{rack_string}'. "
            "Must be one letter followed by a number (e.g., A1, B5, C10)."
        )
    return rack_string.upper()


def format_hosts_for_confirmation(hosts_list: list[str]) -> str:
    """Formats the list of hosts for use in an ask_confirmation message.

    Limits the display to the first 10 hosts with a count of the remainder.
    """
    hosts_number = len(hosts_list)

    # Prepare the list for display (up to 10 hosts)
    if hosts_number > 10:
        displayed_hosts = hosts_list[:10]
        suffix = "\n  ... and {} more host(s)".format(hosts_number - 10)
    else:
        displayed_hosts = hosts_list
        suffix = ""

    # Format the list with bullet points
    hosts_display = "\n  - ".join(displayed_hosts)

    confirmation_message = (
        f"This action will apply to the following {hosts_number} hosts:\n"
        f"  - {hosts_display}{suffix}\n\n"
        f"Do you want to proceed?"
    )
    return confirmation_message


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
        required_group = parser.add_argument_group(title='required arguments')
        required_group.add_argument(
            "--k8s-cluster",
            required=True,
            help="K8s cluster the nodes are part of",
            choices=ALLOWED_CUMIN_ALIASES.keys(),
        )
        actions = parser.add_subparsers(dest="action", help="The action to perform")
        action_pool = actions.add_parser("pool")
        action_pool.add_argument(
            "--downtime-id",
            help="Remove downtime by downtime-id",
        )

        action_depool = actions.add_parser("depool")
        action_depool = enrich_argument_parser_with_downtime_duration(action_depool)

        action_check = actions.add_parser("check")
        for action in (action_pool, action_depool, action_check):
            group = action.add_mutually_exclusive_group(required=True)
            group.add_argument(
                "hosts",
                nargs="?",  # optional positionally, but enforced by the group
                default=None,
                help="Hosts to be pooled/depooled/checked (specified in Cumin query syntax)."
                " Cannot be used with the '--rack' flag."
            )
            group.add_argument(
                "--rack",
                type=validate_rack_format,
                help="Rack name (e.g. C5) to pool/depool/check all worker nodes for."
                " Cannot be used with 'hosts' argument."
            )

        return parser


class PoolDepoolK8sNodesRunner(CookbookRunnerBase):
    """Drain and depool a single host."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Drain a single host and depool it"""
        self.args = args
        self.spicerack = spicerack
        self.k8s_cluster = args.k8s_cluster
        self.actions = self.spicerack.actions
        self.reason = self.spicerack.admin_reason(
            f"{args.action} {args.hosts}" if not args.reason else args.reason
        )
        if args.action != "depool" or (args.minutes == args.hours == args.days == 0):
            self.downtime_duration = None
        else:
            self.downtime_duration = timedelta(
                days=args.days, hours=args.hours, minutes=args.minutes
            )

        hosts_query = args.hosts  # Start with the user-provided host query
        self.hosts_target = f"'{args.hosts}'"  # For the repool message

        if args.rack:
            # k8s_cluster names typically end with the datacenter code (e.g., "wikikube-codfw")
            site_name = self.k8s_cluster.split('-')[-1]
            if site_name not in CORE_DATACENTERS:
                raise ValueError(
                    "Unknown data center provided in k8s_cluster parameter, "
                    "allowed values: " + str(CORE_DATACENTERS)
                )

            # If --rack is provided, use the Netbox query format.
            hosts_query = f'P:netbox::host%location ~ "{args.rack}.*{site_name}"'
            self.hosts_target = f"--rack '{args.rack}'"  # For the repool message

        # Ensure we have a query to run
        if not hosts_query:
            # This should be caught by the args validation, but for safety:
            raise ValueError("Must specify hosts or a rack name.")
        logger.info("Using hosts query: '%s'", hosts_query)

        try:
            self.remote_hosts: RemoteHosts = spicerack.remote().query(
                f"P{{{hosts_query}}} and (A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['workers']} "
                f"or A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['control-plane']})"
            )
        except RemoteError as exc:
            raise RuntimeError(
                f"No host found among any k8s nodes in cluster {self.k8s_cluster}"
                f" for Cumin query: {hosts_query} "
            ) from exc

        # Add a guardrail in case there were too many hosts selected
        if len(self.remote_hosts.hosts) > 1:
            # Use the helper function to format the message
            confirmation_message = format_hosts_for_confirmation(self.remote_hosts.hosts)

            ask_confirmation(confirmation_message)

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
            self.expected_bgp_session_counts[host] = (
                self._get_expected_bgp_session_count(host)
            )

        self.k8s_cli = Kubernetes(
            group=ALLOWED_CUMIN_ALIASES[self.k8s_cluster]["k8s-group"],
            # The cluster name expected here might be different from the one in the cumin alias
            # wikikube is an example of this as we call it wikikube-eqiad in cumin, but eqiad in k8s config
            cluster=ALLOWED_CUMIN_ALIASES[self.k8s_cluster]["k8s-cluster"],
            dry_run=spicerack.dry_run,
        )

        self.phabricator = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        if self.args.action == "check":
            self._action_check()
            # Bail out early, don't log to SAL etc.
            raise CookbookInitSuccess()

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return "{} for host {}".format(self.args.action, self.remote_hosts.hosts)

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.remote_hosts.hosts), concurrency=1, ttl=600)

    def _repool_message(self, downtime_id: str) -> str:
        """Return a message on how to repool the nodes and remove the downtime"""
        extra_args = ""
        if self.args.reason:
            extra_args += f"--reason '{self.args.reason}'"
        if self.args.task_id:
            extra_args += f" --task-id {self.args.task_id}"
        extra_args += " "

        return (
            f"To repool the nodes and remove the downtime, run:\n"
            f"`cookbook sre.k8s.pool-depool-node --k8s-cluster '{self.args.k8s_cluster}' "
            f"{extra_args}pool --downtime-id {downtime_id} '{self.hosts_target}'`\n"
        )

    def post_to_phab(
        self, message: Optional[str] = None, downtime_id: Optional[str] = None
    ) -> None:
        """Comment on the phabricator task if we pool/depool"""
        if self.args.action != "check":
            if message is None:
                message = (
                    f"Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:\n"
                    f"{self.actions}\n"
                )
                if downtime_id:
                    message += self._repool_message(downtime_id)

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

    def _action_check(self):
        """Check the status of the node(s)"""
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

    def _action_depool(self):
        """Cordon, drain and depool the node(s)"""
        downtime_id = None
        if self.downtime_duration is not None:
            alerting_hosts = self.spicerack.alerting_hosts(self.remote_hosts.hosts)
            downtime_id = alerting_hosts.downtime(
                (
                    self.reason
                    if self.args.reason
                    else self.spicerack.admin_reason(
                        "Depooled via sre.k8s.pool-depool-node"
                    )
                ),
                duration=self.downtime_duration,
            )
        logger.info("Depooling %s from %s", self.remote_hosts.hosts, self.k8s_cluster)
        self.confctl.update_objects({"pooled": "inactive"}, self.confctl_services)
        self._k8s_node_action(self.remote_hosts.hosts, "cordon")
        logger.info("Draining %s", self.remote_hosts.hosts)
        self._k8s_node_action(self.remote_hosts.hosts, "drain")
        self.actions[str(self.remote_hosts.hosts)].success(
            f"Host {self.remote_hosts.hosts} depooled from {self.k8s_cluster}"
        )
        logger.info("%s completed:\n%s\n", __name__, self.actions)
        self.post_to_phab(downtime_id=downtime_id)
        if downtime_id is not None:
            logger.info(self._repool_message(downtime_id))

    def _action_pool(self):
        """Uncordon and pool the node(s)"""
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
        if self.args.downtime_id:
            alerting_hosts = self.spicerack.alerting_hosts(self.remote_hosts.hosts)
            alerting_hosts.remove_downtime(self.args.downtime_id)
        logger.info("%s completed:\n%s\n", __name__, self.actions)
        self.post_to_phab()

    def run(self):
        """Uncordon and pool or cordon, drain, and depool the host"""
        # Check action is called from __init__() to bail out early
        if self.args.action == "pool":
            self._action_pool()
        elif self.args.action == "depool":
            self._action_depool()
