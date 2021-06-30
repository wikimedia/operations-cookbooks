#!/usr/bin/env python3
# pylint: disable=unsubscriptable-object,too-many-arguments
"""Cloud Services Cookbooks"""
__title__ = __doc__
import base64
import getpass
import json
import logging
import re
import socket
import time
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Set, Union

import yaml
from cumin.transports import Command
from spicerack import ICINGA_DOMAIN, Spicerack
from spicerack.remote import Remote, RemoteHosts

LOGGER = logging.getLogger(__name__)
PHABRICATOR_BOT_CONFIG_FILE = "/etc/phabricator_ops-monitoring-bot.conf"
AGGREGATES_FILE_PATH = "/etc/wmcs_host_aggregates.yaml"
K8S_SYSTEM_NAMESPACES = [
    "kube-system",
    "metrics",
]
DIGIT_RE = re.compile("([0-9]+)")
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


OpenstackID = str
OpenstackName = str


def _quote(mystr: str) -> str:
    """Wraps the given string in single quotes."""
    return f"'{mystr}'"


class OpenstackError(Exception):
    """Parent class for all openstack related errors."""


class OpenstackNotFound(OpenstackError):
    """Thrown when trying to get an element from Openstack gets no results."""


class OpenstackMigrationError(OpenstackError):
    """Thrown when there's an issue with migration."""


class OpenstackRuleDirection(Enum):
    """Directior for the security group roule."""

    ingress = auto()
    egress = auto()


class OpenstackServerGroupPolicy(Enum):
    """Affinity for the server group."""

    soft_anti_affinity = "soft-anti-affinity"
    anti_affinity = "anti-affinity"
    affinity = "affinity"
    soft_affinity = "soft-affinity"


class OpenstackAPI:
    """Class to interact with the Openstack API (undirectly for now)."""

    def __init__(
        self, remote: Remote, control_node_fqdn: str = "cloudcontrol1003.wikimedia.org", project: OpenstackName = ""
    ):
        """Init."""
        self.project = project
        self.control_node_fqdn = control_node_fqdn
        self._control_node = remote.query(f"D{{{control_node_fqdn}}}", use_sudo=True)

    def _run(
        self, *command: List[str], is_safe: bool = False, capture_errors: bool = False
    ) -> Union[Dict[str, Any], str]:
        """Run an openstack command on a control node.

        Returns the loaded json if able, otherwise the raw output.
        """
        # some commands don't have formatted output
        if "delete" in command:
            format_args = []
        else:
            format_args = ["-f", "json"]

        full_command = [
            "env",
            f"OS_PROJECT_ID={self.project}",
            "wmcs-openstack",
            *command,
            *format_args,
        ]

        command = Command(
            command=" ".join(full_command),
            ok_codes=[0, 1, 2] if capture_errors else [0],
        )
        try:
            result = next(self._control_node.run_sync(command, is_safe=is_safe))

        except StopIteration:
            result = None

        if result is None:
            raw_result = "{}"
        else:
            raw_result = result[1].message().decode()

        try:
            return json.loads(raw_result)

        except json.JSONDecodeError:
            return raw_result

    def server_list(self) -> List[Dict[str, Any]]:
        """Retrieve the list of servers for the project."""
        return self._run("server", "list", is_safe=True)

    def server_delete(self, name_to_remove: OpenstackName) -> None:
        """Delete a server.

        Note that the name_to_remove is the name of the node as registeredin
        Openstack, that's probably not the FQDN (and hopefully the hostname,
        but maybo not).
        """
        self._run("server", "delete", name_to_remove, is_safe=False)

    def server_create(
        self,
        name: OpenstackName,
        flavor: Union[OpenstackID, OpenstackName],
        image: Union[OpenstackID, OpenstackName],
        network: Union[OpenstackID, OpenstackName],
        server_group_id: OpenstackID,
        security_group_ids: List[OpenstackID],
    ) -> None:
        """Create a server.

        Note: You will probably want to add the server to the 'default' security group at least.
        """
        security_group_options = []
        for security_group_id in security_group_ids:
            security_group_options.extend(["--security-group", security_group_id])

        self._run(
            "server",
            "create",
            "--flavor",
            _quote(flavor),
            "--image",
            _quote(image),
            "--network",
            _quote(network),
            "--hint",
            f"group={server_group_id}",
            "--wait",
            *security_group_options,
            name,
        )

    def server_get_aggregates(self, name: OpenstackName) -> List[Dict[str, Any]]:
        """Get all the aggregates for the given server."""
        # NOTE: this currently does a bunch of requests making it slow, can be simplified
        # once the following gets released:
        #  https://review.opendev.org/c/openstack/python-openstackclient/+/794237
        current_aggregates = self.aggregate_list()
        server_aggregates: List[Dict[str, any]] = []
        for aggregate in current_aggregates:
            aggregate_details = self.aggregate_show(aggregate=aggregate["Name"])
            if name in aggregate_details.get("hosts", []):
                server_aggregates.append(aggregate_details)

        return server_aggregates

    def security_group_list(self) -> List[Dict[str, Any]]:
        """Retrieve the list of security groups."""
        return self._run("security", "group", "list", is_safe=True)

    def security_group_create(self, name: OpenstackName, description: str) -> None:
        """Create a security group."""
        self._run("security", "group", "create", name, "--description", description)

    def security_group_rule_create(
        self,
        direction: OpenstackRuleDirection,
        remote_group: OpenstackName,
        security_group: OpenstackName,
    ) -> None:
        """Create a rule inside the given security group."""
        self._run(
            "security",
            "group",
            "rule",
            "create",
            f"--{direction.name}",
            "--remote-group",
            remote_group,
            "--protocol",
            "any",
            security_group,
        )

    def security_group_ensure(
        self, security_group: OpenstackName, description: str = "Security group created from spicerack."
    ) -> None:
        """Make sure that the given security group exists, create it if not there."""
        try:
            self.security_group_by_name(name=security_group)
            LOGGER.info("Security group %s already exists, not creating.", security_group)

        except OpenstackNotFound:
            LOGGER.info("Creating security group %s...", security_group)
            self.security_group_create(
                name=security_group,
                description=description,
            )
            self.security_group_rule_create(
                direction=OpenstackRuleDirection.egress,
                remote_group=security_group,
                security_group=security_group,
            )
            self.security_group_rule_create(
                direction=OpenstackRuleDirection.ingress,
                remote_group=security_group,
                security_group=security_group,
            )

    def security_group_by_name(self, name: OpenstackName) -> Optional[Dict[str, Any]]:
        """Retrieve the security group info given a name.

        Raises OpenstackNotFound if there's no security group found for the given name in the current project.
        """
        existing_security_groups = self.security_group_list()
        for security_group in existing_security_groups:
            if security_group["Project"] == self.project:
                if security_group["Name"] == name:
                    return security_group

        raise OpenstackNotFound(f"Unable to find a security group with name {name}")

    def server_group_list(self) -> List[Dict[str, Any]]:
        """Get the list of server groups.

        Note:  it seems that on cli the project flag shows nothing :/ so we get the list all of them.
        """
        return self._run("server", "group", "list", is_safe=True)

    def server_group_create(self, name: OpenstackName, policy: OpenstackServerGroupPolicy) -> None:
        """Create a server group."""
        self._run(
            "--os-compute-api-version=2.15",  # needed to be 2.15 or higher for soft-* policies
            "server",
            "group",
            "create",
            "--policy",
            policy.value,
            name,
        )

    def server_group_ensure(
        self, server_group: OpenstackName, policy: OpenstackServerGroupPolicy = OpenstackServerGroupPolicy.anti_affinity
    ) -> None:
        """Make sure that the given server group exists, create it if not there."""
        try:
            self.server_group_by_name(name=server_group)
            LOGGER.info("Server group %s already exists, not creating.", server_group)
        except OpenstackNotFound:
            self.server_group_create(policy=policy, name=server_group)

    def server_group_by_name(self, name: OpenstackName) -> Optional[Dict[str, Any]]:
        """Retrieve the server group info given a name.

        Raises OpenstackNotFound if thereś no server group found with the given name.
        """
        all_server_groups = self.server_group_list()
        for server_group in all_server_groups:
            if server_group.get("Name", "") == name:
                return server_group

        raise OpenstackNotFound(f"Unable to find a server group with name {name}")

    def aggregate_list(self) -> List[Dict[str, Any]]:
        """Get the simplified list of aggregates."""
        return self._run("aggregate", "list", "--long", is_safe=True)

    def aggregate_show(self, aggregate: Union[OpenstackName, OpenstackID]) -> List[Dict[str, Any]]:
        """Get the details of a given aggregate."""
        return self._run("aggregate", "show", aggregate, is_safe=True)

    def aggregate_remove_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Remove the given host from the aggregate."""
        result = self._run("aggregate", "remove", "host", aggregate_name, host_name, capture_errors=True)
        if "HTTP 404" in result:
            raise OpenstackNotFound(
                f"Node {host_name} was not found in aggregate {aggregate_name}, did you try using the hostname "
                "instead of the fqdn?"
            )

    def aggregate_add_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Add the given host to the aggregate."""
        result = self._run("aggregate", "add", "host", aggregate_name, host_name, capture_errors=True)
        if "HTTP 404" in result:
            raise OpenstackNotFound(
                f"Node {host_name} was not found in aggregate {aggregate_name}, did you try using the hostname "
                "instead of the fqdn?"
            )

    def aggregate_persist_on_host(self, host: RemoteHosts) -> None:
        """Creates a file in the host with it's current list of aggregates.

        For later usage, for example, when moving the host temporarily to another aggregate.
        """
        hostname = str(host).split(".", 1)[0]
        current_aggregates = self.server_get_aggregates(name=hostname)
        simple_create_file(
            dst_node=host,
            contents=yaml.dump(current_aggregates, indent=4),
            remote_path=AGGREGATES_FILE_PATH,
        )

    @staticmethod
    def aggregate_load_from_host(host: RemoteHosts) -> None:
        """Loads the persisted list of aggregates from the host."""
        try:
            result = next(host.run_sync(f"cat {AGGREGATES_FILE_PATH}", is_safe=True))[1].message().decode()

        except Exception as error:
            raise OpenstackNotFound(f"Unable to cat the file {AGGREGATES_FILE_PATH} on host {host}") from error

        return yaml.safe_load(result)

    def drain_hypervisor(self, hypervisor_name: OpenstackName) -> None:
        """Drain a hypervisor."""
        command = Command(
            command=f"bash -c 'source /root/novaenv.sh && wmcs-drain-hypervisor {hypervisor_name}'",
            timeout=SECONDS_IN_MINUTE * MINUTES_IN_HOUR * 2,
        )
        try:
            next(self._control_node.run_sync(command, is_safe=False))

        except StopIteration:
            raise OpenstackMigrationError(
                f"Got no result when running {command} on {self.control_node_fqdn}, was expecting some output at "
                "least."
            )


class CephException(Exception):
    """Parent exception for all ceph related issues."""


class CephClusterUnhealthy(CephException):
    """Risen when trying to act on an unhealthy cluster."""


class CephFlagSetError(CephException):
    """Risen when something failed when setting a flag in the cluster."""


@dataclass(frozen=True)
class CephClusterSatus:
    """Status of a CEPH cluster."""

    status_dict: Dict[str, Any]

    def get_osdmap_set_flags(self) -> Set[str]:
        """Get osdmap set flags."""
        osd_maps = self.status_dict["health"]["checks"].get("OSDMAP_FLAGS")
        if not osd_maps:
            return []

        raw_flags_line = osd_maps["summary"]["message"]
        if "flag" not in raw_flags_line:
            return []

        # ex: "noout,norebalance flag(s) set"
        flags = raw_flags_line.split(" ")[0].split(",")
        return set(flags)

    @staticmethod
    def _filter_out_octopus_upgrade_warns(status: Dict[str, Any]) -> Dict[str, Any]:
        # ignore temporary alert for octopus upgrade
        # https://docs.ceph.com/en/latest/security/CVE-2021-20288/#recommendations
        new_status = deepcopy(status)
        if "AUTH_INSECURE_GLOBAL_ID_RECLAIM" in new_status["health"]["checks"]:
            del new_status["health"]["checks"]["AUTH_INSECURE_GLOBAL_ID_RECLAIM"]

        if "AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED" in new_status["health"]["checks"]:
            del new_status["health"]["checks"]["AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED"]

        if len(new_status["health"]["checks"]) == 0:
            new_status["health"]["status"] = "HEALTH_OK"

        return new_status

    def is_cluster_status_just_maintenance(self) -> bool:
        """Return if the cluster is in HEALTH_WARN only because it's in maintenance status."""
        # ignore temporary alert for octopus upgrade
        # https://docs.ceph.com/en/latest/security/CVE-2021-20288/#recommendations
        temp_status = self._filter_out_octopus_upgrade_warns(self.status_dict)

        if temp_status["health"]["status"] != "HEALTH_WARN":
            return False

        if "OSDMAP_FLAGS" in temp_status["health"]["checks"] and len(temp_status["health"]["checks"]) == 1:
            current_flags = self.get_osdmap_set_flags()
            return current_flags == {"noout", "norebalance"}

        return False

    def check_healthy(self, consider_maintenance_healthy: bool = False) -> None:
        """Check if the cluster is healthy."""
        # ignore temporary alert for octopus upgrade
        # https://docs.ceph.com/en/latest/security/CVE-2021-20288/#recommendations
        temp_status = self._filter_out_octopus_upgrade_warns(self.status_dict)

        if temp_status["health"]["status"] == "HEALTH_OK":
            return

        if consider_maintenance_healthy and self.is_cluster_status_just_maintenance():
            return

        if temp_status["health"]["status"] != "HEALTH_OK":
            raise CephClusterUnhealthy(
                f"The cluster is currently in an unhealthy status: \n{json.dumps(self.status_dict['health'], indent=4)}"
            )


class CephClusterController:
    """Controller for a CEPH cluster."""

    def __init__(self, remote: Remote, controlling_node_fqdn: str):
        """Init."""
        self._remote = remote
        self._controlling_node_fqdn = controlling_node_fqdn
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)

    def get_nodes(self) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        raw_output = next(self._controlling_node.run_sync("ceph node ls -f json"))[1].message().decode()
        # There's usually a couple empty lines before the json data
        return json.loads(raw_output.splitlines()[-1])

    def get_nodes_domain(self) -> str:
        """Get the network domain for the nodes in the cluster."""
        return self._controlling_node_fqdn.split(".", 1)[-1]

    def change_controlling_node(self) -> None:
        """Change the current node being used to interact with the cluster for another one."""
        current_monitor_name = self._controlling_node_fqdn.split(".", 1)[0]
        nodes = self.get_nodes()
        another_monitor = next(
            node_host for node_name, node_host in nodes["mon"].items() if node_name != current_monitor_name
        )[0]
        self._controlling_node_fqdn = f"{another_monitor}.{self.get_nodes_domain()}"
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)
        LOGGER.info("Changed to node %s to control the CEPH cluster.", self._controlling_node_fqdn)

    def get_cluster_status(self) -> CephClusterSatus:
        """Get the current cluster status."""
        raw_cluster_status = next(self._controlling_node.run_sync("ceph status -f json"))[1].message().decode()
        return CephClusterSatus(status_dict=json.loads(raw_cluster_status))

    def set_osdmap_flag(self, flag_name: str) -> None:
        """Set one of the osdmap flags."""
        set_osdmap_flag_result = (
            next(self._controlling_node.run_sync(f"ceph osd set {flag_name}"))[1].message().decode()
        )
        if set_osdmap_flag_result != f"{flag_name} is set":
            raise CephFlagSetError(f"Unable to set `{flag_name}` on the cluster: {set_osdmap_flag_result}")

    def unset_osdmap_flag(self, flag_name: str) -> None:
        """Unset one of the osdmap flags."""
        unset_osdmap_flag_result = (
            next(self._controlling_node.run_sync(f"ceph osd unset {flag_name}"))[1].message().decode()
        )
        if unset_osdmap_flag_result != f"{flag_name} is unset":
            raise CephFlagSetError(f"Unable to unset `{flag_name}` on the cluster: {unset_osdmap_flag_result}")

    def set_maintenance(self, force: bool = False) -> None:
        """Set maintenance."""
        cluster_status = self.get_cluster_status()
        if cluster_status.is_cluster_status_just_maintenance():
            LOGGER.info("Cluster already in maintenance status.")
            return

        try:
            cluster_status.check_healthy()

        except CephClusterUnhealthy:
            if not force:
                LOGGER.warning(
                    "Cluster is not in a healthy status, putting it in maintenance might stop any recovery processes. "
                    "Use --force to ignore this message and set the cluster in maintenance mode anyhow."
                )
                raise

            LOGGER.info(
                (
                    "Cluster is not in a healthy status, putting it in maintenance might stop any recovery processes. "
                    "Continuing as --force was specified. Current status:\n%s"
                ),
                json.dumps(cluster_status.status_dict["health"], indent=4),
            )

        self.set_osdmap_flag(flag_name="noout")
        self.set_osdmap_flag(flag_name="norebalance")

    def unset_maintenance(self, force: bool = False) -> None:
        """Unset maintenance."""
        cluster_status = self.get_cluster_status()
        try:
            cluster_status.check_healthy(consider_maintenance_healthy=True)

        except CephClusterUnhealthy:
            if not force:
                LOGGER.warning(
                    "Cluster is not in a healthy status, getting it out of maintenance might have undesirable "
                    "effects. Use --force to ignore this message and unset the cluster maintenance mode anyhow."
                )
                raise

            LOGGER.info(
                (
                    "Cluster is not in a healthy status, getting it out of maintenance might have undesirable "
                    "state. Continuing as --force was specified. Current status: \n%s"
                ),
                json.dumps(cluster_status.status_dict["health"], indent=4),
            )

        if cluster_status.is_cluster_status_just_maintenance():
            self.unset_osdmap_flag(flag_name="noout")
            self.unset_osdmap_flag(flag_name="norebalance")

        else:
            LOGGER.info("Cluster already out of maintenance status.")

    def wait_for_cluster_healthy(self, consider_maintenance_healthy: bool = False, timeout_seconds: int = 600) -> None:
        """Wait until a cluster becomes healthy."""
        check_interval_seconds = 10
        start_time = time.time()
        cur_time = start_time
        while cur_time - start_time < timeout_seconds:
            try:
                self.get_cluster_status().check_healthy(consider_maintenance_healthy=consider_maintenance_healthy)
                return

            except CephClusterUnhealthy:
                LOGGER.info(
                    "Cluster still not healthy, waiting another %d (timeout=%d)...",
                    check_interval_seconds,
                    timeout_seconds,
                )

            time.sleep(check_interval_seconds)
            cur_time = time.time()

        cluster_status = self.get_cluster_status()
        raise CephClusterUnhealthy(
            f"Waited {timeout_seconds} for the cluster to become healthy, but it never did, current state:\n"
            f"\n{json.dumps(cluster_status.status_dict['health'], indent=4)}"
        )


class KubernetesError(Exception):
    """Parent class for all kubernetes related errors."""


class KubernetesMalformedCluterInfo(KubernetesError):
    """Risen when the gotten cluster info is not formatted as expected."""


class KubernetesNodeNotFound(KubernetesError):
    """Risen when the given node does not exist."""


class KubernetesNodeStatusError(KubernetesError):
    """Risen when the given node status is not recognized."""


@dataclass(frozen=True)
class KubernetesClusterInfo:
    """Kubernetes cluster info."""

    master_url: str
    dns_url: str
    metrics_url: str

    @classmethod
    def form_cluster_info_output(cls, raw_output: str) -> "KubernetesClusterInfo":
        """Create the object from the cli 'kubectl cluster-info' output.

        Example of output:
        ```
        Kubernetes master is running at https://k8s.toolsbeta.eqiad1.wikimedia.cloud:6443  # noqa: E501
        KubeDNS is running at https://k8s.toolsbeta.eqiad1.wikimedia.cloud:6443/api/v1/namespaces/kube-system/services/kube-dns:dns/proxy  # noqa: E501
        Metrics-server is running at https://k8s.toolsbeta.eqiad1.wikimedia.cloud:6443/api/v1/namespaces/kube-system/services/https:metrics-server:/proxy  # noqa: E501

        To further debug and diagnose cluster problems, use 'kubectl cluster-info dump'.
        ```
        """
        master_url = None
        dns_url = None
        metrics_url = None
        for line in raw_output.splitlines():
            # get rid of the terminal colors
            line = line.replace("\x1b[0;33m", "").replace("\x1b[0;32m", "").replace("\x1b[0m", "")
            # k8s <1.20 uses "master", >=1.20 uses "control plane":
            #   https://github.com/kubernetes/kubernetes/commit/ab129349acadb4539cc8c584e4f9a43dd8b45761
            if line.startswith("Kubernetes master") or line.startswith("Kubernetes control plane"):
                master_url = line.rsplit(" ", 1)[-1]
            elif line.startswith("KubeDNS"):
                dns_url = line.rsplit(" ", 1)[-1]
            elif line.startswith("Metrics-server"):
                metrics_url = line.rsplit(" ", 1)[-1]

        if master_url is None or dns_url is None or metrics_url is None:
            raise KubernetesMalformedCluterInfo(f"Unable to parse cluster info:\n{raw_output}")

        return cls(
            master_url=master_url,
            dns_url=dns_url,
            metrics_url=metrics_url,
        )


class KubernetesController:
    """Controller for a kubernetes cluster."""

    def __init__(self, remote: Remote, controlling_node_fqdn: str):
        """Init."""
        self._remote = remote
        self.controlling_node_fqdn = controlling_node_fqdn
        self._controlling_node = self._remote.query(f"D{{{self.controlling_node_fqdn}}}", use_sudo=True)

    def get_nodes_domain(self) -> str:
        """Get the network domain for the nodes in the cluster."""
        return self.controlling_node_fqdn.split(".", 1)[-1]

    def get_cluster_info(self) -> KubernetesClusterInfo:
        """Get cluster info."""
        raw_output = (
            # cluster-info does not support json output format (there's a dump
            # command, but it's too verbose)
            next(self._controlling_node.run_sync("kubectl cluster-info"))[1]
            .message()
            .decode()
        )
        return KubernetesClusterInfo.form_cluster_info_output(raw_output=raw_output)

    def get_nodes(self, selector: Optional[str] = None) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        if selector:
            selector_cli = f"--selector='{selector}'"
        else:
            selector_cli = ""

        raw_output = (
            next(self._controlling_node.run_sync(f"kubectl get nodes --output=json {selector_cli}"))[1]
            .message()
            .decode()
        )
        return json.loads(raw_output)["items"]

    def get_node(self, node_hostname: str) -> Dict[str, Any]:
        """Get only info for the the given node."""
        return self.get_nodes(selector=f"kubernetes.io/hostname={node_hostname}")

    def get_pods(self, field_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get pods."""
        if field_selector:
            field_selector_cli = f"--field-selector='{field_selector}'"
        else:
            field_selector_cli = ""

        raw_output = (
            next(self._controlling_node.run_sync(f"kubectl get pods --output=json {field_selector_cli}"))[1]
            .message()
            .decode()
        )
        return json.loads(raw_output)["items"]

    def get_pods_for_node(self, node_hostname: str) -> Dict[str, Any]:
        """Get pods for node."""
        return self.get_pods(field_selector=f"spec.nodeName={node_hostname}")

    def drain_node(self, node_hostname: str) -> Dict[str, Any]:
        """Drain a node, it does not wait for the containers to be stopped though."""
        self._controlling_node.run_sync(f"kubectl drain --ignore-daemonsets --delete-local-data {node_hostname}")

    def delete_node(self, node_hostname: str) -> Dict[str, Any]:
        """Delete a node, it does not drain it, see drain_node for that."""
        current_nodes = self.get_nodes(selector=f"kubernetes.io/hostname={node_hostname}")
        if not current_nodes:
            LOGGER.info("Node %s was not part of this kubernetes cluster, ignoring", node_hostname)

        self._controlling_node.run_sync(f"kubectl delete node {node_hostname}")

    def is_node_ready(self, node_hostname: str) -> bool:
        """Ready means in 'Ready' status."""
        node_info = self.get_node(node_hostname=node_hostname)
        if not node_info:
            raise KubernetesNodeNotFound("Unable to find node {node_hostname} in the cluster.")

        try:
            return next(
                condition["status"] == "True"
                for condition in node_info[0]["status"]["conditions"]
                if condition["type"] == "Ready"
            )
        except StopIteration:
            raise KubernetesNodeStatusError(
                f"Unable to get 'Ready' condition of node {node_hostname}, got conditions:\n"
                f"{node_info[0]['conditions']}"
            )


class KubeadmError(Exception):
    """Parent class for all kubeadm related errors."""


class KubeadmDeleteTokenError(KubeadmError):
    """Raised when there was an error deleting a token."""


class KubeadmCreateTokenError(KubeadmError):
    """Raised when there was an error creating a token."""


class KubeadmTimeoutForNodeReady(KubeadmError):
    """Raised when a node did not get to Ready status on time."""


class KubeadmController:
    """Controller for a Kubeadmin managed kubernetes cluster."""

    def __init__(self, remote: Remote, controlling_node_fqdn: str):
        """Init."""
        self._remote = remote
        self._controlling_node_fqdn = controlling_node_fqdn
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)

    def get_nodes_domain(self) -> str:
        """Get the network domain for the nodes in the cluster."""
        return self._controlling_node_fqdn.split(".", 1)[-1]

    def get_new_token(self) -> str:
        """Creates a new bootstrap token."""
        raw_output = next(self._controlling_node.run_sync("kubeadm token create"))[1].message().decode()
        output = raw_output.splitlines()[-1].strip()
        if not output:
            raise KubeadmCreateTokenError(f"Error creating a new token:\nOutput:{raw_output}")

        return output

    def delete_token(self, token: str) -> str:
        """Removes the given bootstrap token."""
        raw_output = next(self._controlling_node.run_sync(f"kubeadm token delete {token}"))[1].message().decode()
        if "deleted" not in raw_output:
            raise KubeadmDeleteTokenError(f"Error deleting token {token}:\nOutput:{raw_output}")

        return raw_output.strip()

    def get_ca_cert_hash(self) -> str:
        """Retrieves the CA cert hash to use when bootstrapping."""
        raw_output = (
            next(
                self._controlling_node.run_sync(
                    "openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt "
                    "| openssl rsa -pubin -outform der 2>/dev/null "
                    "| openssl dgst -sha256 -hex "
                    "| sed 's/^.* //'"
                )
            )[1]
            .message()
            .decode()
        )
        return raw_output.strip()

    def join(
        self,
        kubernetes_controller: KubernetesController,
        wait_for_ready: bool = True,
        timeout_seconds: int = 600,
    ) -> None:
        """Join this node to the kubernetes cluster controlled by the given controller."""
        control_kubeadm = KubeadmController(
            remote=self._remote,
            controlling_node_fqdn=kubernetes_controller.controlling_node_fqdn,
        )
        cluster_info = kubernetes_controller.get_cluster_info()
        # kubeadm does not want the protocol part https?://
        join_address = cluster_info.master_url.split("//", 1)[-1]
        ca_cert_hash = control_kubeadm.get_ca_cert_hash()
        new_token = control_kubeadm.get_new_token()
        try:
            self._controlling_node.run_sync(
                f"kubeadm join {join_address} "
                f"--token {new_token} "
                f"--discovery-token-ca-cert-hash sha256:{ca_cert_hash}"
            )

            if not wait_for_ready:
                return

            new_node_hostname = self._controlling_node_fqdn.split(".", 1)[0]
            check_interval_seconds = 10
            start_time = time.time()
            cur_time = start_time
            while cur_time - start_time < timeout_seconds:
                if kubernetes_controller.is_node_ready(node_hostname=new_node_hostname):
                    return

                time.sleep(check_interval_seconds)
                cur_time = time.time()

            cur_conditions = kubernetes_controller.get_node(node_hostname=new_node_hostname)[0]["conditions"]
            raise KubeadmTimeoutForNodeReady(
                f"Waited {timeout_seconds} for the node {new_node_hostname} to "
                "become healthy, but it never did. Current conditions:\n"
                f"{json.dumps(cur_conditions, indent=4)}"
            )

        finally:
            control_kubeadm.delete_token(token=new_token)


def simple_create_file(
    dst_node: RemoteHosts,
    contents: str,
    remote_path: str,
    use_root: bool = True,
) -> None:
    """Creates a file on the remote host/hosts with the given content."""
    # this makes it esier to get away with quotes or similar
    base64_content = base64.b64encode(contents.encode("utf8"))
    full_command = [
        "echo",
        f"'{base64_content.decode()}'",
        "|",
        "base64",
        "--decode",
        "|",
    ]
    if use_root:
        full_command.extend(["sudo", "-i"])

    full_command.extend(["tee", remote_path])

    return next(dst_node.run_sync(" ".join(full_command)))[1].message().decode()


def natural_sort_key(element: str) -> List[Union[str, int]]:
    """Changes "name-12.something.com" into ["name-", 12, ".something.com"]."""
    return [int(mychunk) if mychunk.isdigit() else mychunk for mychunk in DIGIT_RE.split(element)]


def wrap_with_sudo_icinga(my_spicerack: Spicerack) -> Spicerack:
    """Wrap spicerack icinga to allow sudo.

    We have to patch the master host to allow sudo, all this weirdness is
    because icinga_master_host is an @property and can't be patched on
    the original instance
    """

    class SudoIcingaSpicerackWrapper(Spicerack):
        """Dummy wrapper class to allow sudo icinga."""

        def __init__(self):  # pylint: disable-msg=super-init-not-called
            """Init."""

        @property
        def icinga_master_host(self) -> RemoteHosts:
            """Icinga master host."""
            new_host = self.remote().query(
                query_string=self.dns().resolve_cname(ICINGA_DOMAIN),
                use_sudo=True,
            )
            return new_host

        def __getattr__(self, what):
            return getattr(my_spicerack, what)

        def __setattr__(self, what, value):
            return setattr(my_spicerack, what, value)

    return SudoIcingaSpicerackWrapper()


def dologmsg(
    message: str,
    project: str,
    task_id: Optional[str] = None,
    channel: str = "#wikimedia-cloud",
    host: str = "wm-bot.wm-bot.wmcloud.org",
    port: int = 64835,
):
    """Log a message to the given irc channel for stashbot to pick up and register in SAL."""
    postfix = f"- cookbook ran by {getpass.getuser()}@{socket.gethostname()}"
    if task_id is not None:
        postfix = f"({task_id}) {postfix}"

    payload = f"{channel} !log {project} {message} {postfix}\n"
    # try all the possible addresses for that host (ip4/ip6/etc.)
    for family, s_type, proto, _, sockaddr in socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP):
        my_socket = socket.socket(family, s_type, proto)
        my_socket.connect(sockaddr)
        try:
            my_socket.send(payload.encode("utf-8"))
            LOGGER.info(message)
            return
        # pylint: disable=broad-except
        except Exception as error:
            LOGGER.warning("Error trying to send a message to %s: %s", str(sockaddr), str(error))
        finally:
            my_socket.close()

    raise Exception(f"Unable to send log message to {host}:{port}, see previous logs for details")
