#!/usr/bin/env python3
# pylint: disable=too-many-arguments,too-many-lines
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
from itertools import chain
from typing import Any, Dict, List, Optional, Set, Union
from unittest import mock

import yaml
from defusedxml import ElementTree
from ClusterShell.MsgTree import MsgTreeElem
from cumin.transports import Command
from spicerack import ICINGA_DOMAIN, Spicerack
from spicerack.remote import Remote, RemoteHosts
from spicerack.puppet import PuppetHosts
from wmflib.interactive import ask_confirmation

LOGGER = logging.getLogger(__name__)
PHABRICATOR_BOT_CONFIG_FILE = "/etc/phabricator_ops-monitoring-bot.conf"
AGGREGATES_FILE_PATH = "/etc/wmcs_host_aggregates.yaml"
K8S_SYSTEM_NAMESPACES = ["kube-system", "metrics"]
DIGIT_RE = re.compile("([0-9]+)")
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


OpenstackID = str
OpenstackName = str
OpenstackIdentifier = Union[OpenstackID, OpenstackName]


class DebianVersion(Enum):
    """Represents Debian release names/numbers."""

    STRETCH = "09"
    BUSTER = "10"


class OutputFormat(Enum):
    """Types of format supported to try to decode when running commands."""

    JSON = auto()
    YAML = auto()


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

    INGRESS = auto()
    EGRESS = auto()


class OpenstackServerGroupPolicy(Enum):
    """Affinity for the server group."""

    SOFT_ANTI_AFFINITY = "soft-anti-affinity"
    ANTI_AFFINITY = "anti-affinity"
    AFFINITY = "affinity"
    SOFT_AFFINITY = "soft-affinity"


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
        self, *command: List[str], is_safe: bool = False, capture_errors: bool = False, **kwargs
    ) -> Union[Dict[str, Any], str]:
        """Run an openstack command on a control node.

        Returns the loaded json if able, otherwise the raw output.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        # some commands don't have formatted output
        if (
            "delete" in command
            or ("volume" in command and "add" in command)
            or ("set" in command and "port" in command)
        ):
            format_args = []
        else:
            format_args = ["-f", "json"]

        full_command = ["env", f"OS_PROJECT_ID={self.project}", "wmcs-openstack", *command, *format_args]

        return run_one(
            command=full_command, node=self._control_node, is_safe=is_safe, capture_errors=capture_errors, **kwargs
        )

    def create_service_ip(self, ip_name: OpenstackName, network: OpenstackIdentifier, **kwargs) -> Dict[str, Any]:
        """Create a service IP with a specified name

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("port", "create", "--network", _quote(network), _quote(ip_name), **kwargs)

    def attach_service_ip(
        self, ip_address: OpenstackIdentifier, server_port_id: OpenstackIdentifier, **kwargs
    ) -> Dict[str, Any]:
        """Attach a specified service ip address to the specifed port

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run(
            "port", "set", "--allowed-address", f"ip-address={ip_address}", _quote(server_port_id), **kwargs
        )

    def port_get(self, ip_address, **kwargs) -> List[Dict[str, Any]]:
        """Get port for specified IP address"""
        ip_filter = '--fixed-ip="ip-address=%s"' % ip_address
        return self._run("port", "list", ip_filter, **kwargs)

    def zone_get(self, name, **kwargs) -> List[Dict[str, Any]]:
        """Get zone record for specified dns zone"""
        return self._run("zone", "list", "--name", name, **kwargs)

    def recordset_create(self, zone_id, record_type, name, record, **kwargs) -> List[Dict[str, Any]]:
        """Get zone record for specified dns zone"""
        return self._run("recordset", "create", "--type", record_type, "--record", record, zone_id, name, **kwargs)

    def server_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Retrieve the list of servers for the project.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("server", "list", is_safe=True, **kwargs)

    def server_exists(self, hostname: str, **kwargs) -> bool:
        """Returns True if a server exists, False otherwise.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        listing = self.server_list(**kwargs)

        if not any(info for info in listing if info["Name"] == hostname):
            return False

        return True

    def server_delete(self, name_to_remove: OpenstackName) -> None:
        """Delete a server.

        Note that the name_to_remove is the name of the node as registeredin
        Openstack, that's probably not the FQDN (and hopefully the hostname,
        but maybo not).
        """
        self._run("server", "delete", name_to_remove, is_safe=False)

    def volume_create(self, name: OpenstackName, size: int) -> str:
        """Create a volume and return the ID of the created volume.

        --size is in GB
        """
        out = self._run("volume", "create", "--size", str(size), "--type", "standard", name)
        return out["id"]

    def volume_attach(self, server_id: str, volume_id: str) -> None:
        """Attach a volume to a server"""
        self._run("server", "add", "volume", server_id, volume_id)

    def server_create(
        self,
        name: OpenstackName,
        flavor: OpenstackIdentifier,
        image: OpenstackIdentifier,
        network: OpenstackIdentifier,
        server_group_id: OpenstackID,
        security_group_ids: List[OpenstackID],
    ) -> OpenstackIdentifier:
        """Create a server and return the ID of the created server.

        Note: You will probably want to add the server to the 'default' security group at least.
        """
        security_group_options = []
        for security_group_id in security_group_ids:
            security_group_options.extend(["--security-group", security_group_id])

        out = self._run(
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
        return out["id"]

    def server_get_aggregates(self, name: OpenstackName) -> List[Dict[str, Any]]:
        """Get all the aggregates for the given server."""
        # NOTE: this currently does a bunch of requests making it slow, can be simplified
        # once the following gets released:
        #  https://review.opendev.org/c/openstack/python-openstackclient/+/794237
        current_aggregates = self.aggregate_list(print_output=False)
        server_aggregates: List[Dict[str, any]] = []
        for aggregate in current_aggregates:
            aggregate_details = self.aggregate_show(aggregate=aggregate["Name"])
            if name in aggregate_details.get("hosts", []):
                server_aggregates.append(aggregate_details)

        return server_aggregates

    def security_group_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Retrieve the list of security groups.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("security", "group", "list", is_safe=True, **kwargs)

    def security_group_create(self, name: OpenstackName, description: str) -> None:
        """Create a security group."""
        self._run("security", "group", "create", name, "--description", description)

    def security_group_rule_create(
        self, direction: OpenstackRuleDirection, remote_group: OpenstackName, security_group: OpenstackName
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
            self.security_group_by_name(name=security_group, print_output=False)
            LOGGER.info("Security group %s already exists, not creating.", security_group)

        except OpenstackNotFound:
            LOGGER.info("Creating security group %s...", security_group)
            self.security_group_create(name=security_group, description=description)
            self.security_group_rule_create(
                direction=OpenstackRuleDirection.EGRESS, remote_group=security_group, security_group=security_group
            )
            self.security_group_rule_create(
                direction=OpenstackRuleDirection.INGRESS, remote_group=security_group, security_group=security_group
            )

    def security_group_by_name(self, name: OpenstackName, **kwargs) -> Optional[Dict[str, Any]]:
        """Retrieve the security group info given a name.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.

        Raises OpenstackNotFound if there's no security group found for the given name in the current project.
        """
        existing_security_groups = self.security_group_list(**kwargs)
        for security_group in existing_security_groups:
            if security_group["Project"] == self.project:
                if security_group["Name"] == name:
                    return security_group

        raise OpenstackNotFound(f"Unable to find a security group with name {name}")

    def server_group_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Get the list of server groups.

        Note:  it seems that on cli the project flag shows nothing :/ so we get the list all of them.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("server", "group", "list", is_safe=True, **kwargs)

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
        self, server_group: OpenstackName, policy: OpenstackServerGroupPolicy = OpenstackServerGroupPolicy.ANTI_AFFINITY
    ) -> None:
        """Make sure that the given server group exists, create it if not there."""
        try:
            self.server_group_by_name(name=server_group, print_output=False)
            LOGGER.info("Server group %s already exists, not creating.", server_group)
        except OpenstackNotFound:
            self.server_group_create(policy=policy, name=server_group)

    def server_group_by_name(self, name: OpenstackName, **kwargs) -> Optional[Dict[str, Any]]:
        """Retrieve the server group info given a name.

        Raises OpenstackNotFound if thereś no server group found with the given name.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        all_server_groups = self.server_group_list(**kwargs)
        for server_group in all_server_groups:
            if server_group.get("Name", "") == name:
                return server_group

        raise OpenstackNotFound(f"Unable to find a server group with name {name}")

    def aggregate_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Get the simplified list of aggregates.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("aggregate", "list", "--long", is_safe=True, **kwargs)

    def aggregate_show(self, aggregate: OpenstackIdentifier, **kwargs) -> List[Dict[str, Any]]:
        """Get the details of a given aggregate.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self._run("aggregate", "show", aggregate, is_safe=True, **kwargs)

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

    def aggregate_persist_on_host(self, host: RemoteHosts, **kwargs) -> None:
        """Creates a file in the host with it's current list of aggregates.

        For later usage, for example, when moving the host temporarily to another aggregate.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        hostname = str(host).split(".", 1)[0]
        current_aggregates = self.server_get_aggregates(name=hostname, **kwargs)
        simple_create_file(
            dst_node=host, contents=yaml.dump(current_aggregates, indent=4), remote_path=AGGREGATES_FILE_PATH
        )

    @staticmethod
    def aggregate_load_from_host(host: RemoteHosts) -> None:
        """Load the persisted list of aggregates from the host."""
        try:
            result = run_one(
                command=["cat", AGGREGATES_FILE_PATH], node=host, is_safe=True, try_format=OutputFormat.YAML
            )

        except Exception as error:
            raise OpenstackNotFound(f"Unable to cat the file {AGGREGATES_FILE_PATH} on host {host}") from error

        return result

    def drain_hypervisor(self, hypervisor_name: OpenstackName) -> None:
        """Drain a hypervisor."""
        command = Command(
            command=f"bash -c 'source /root/novaenv.sh && wmcs-drain-hypervisor {hypervisor_name}'",
            timeout=SECONDS_IN_MINUTE * MINUTES_IN_HOUR * 2,
        )
        result = run_one(command=command, node=self._control_node, is_safe=False)

        if not result:
            raise OpenstackMigrationError(
                f"Got no result when running {command} on {self.control_node_fqdn}, was expecting some output at "
                "least."
            )


class CephException(Exception):
    """Parent exception for all ceph related issues."""


class CephClusterUnhealthy(CephException):
    """Risen when trying to act on an unhealthy cluster."""


class CephTimeout(CephException):
    """Risen when trying to act on an unhealthy cluster."""


class CephFlagSetError(CephException):
    """Risen when something failed when setting a flag in the cluster."""


class CephNoControllerNode(CephException):
    """Risen when there was no other controlling node found."""


class CephMalformedInfo(CephException):
    """Risen when the output of a command is not what was expected."""


class CephOSDFlag(Enum):
    """Possible OSD flags."""

    # cluster marked as full and stops serving writes
    FULL = "full"
    # stop serving writes and reads
    PAUSE = "pause"
    # avoid marking osds as up (serving traffic)
    NOUP = "noup"
    # avoid marking osds as down (stop serving traffic)
    NODOWN = "nodown"
    # avoid marking osds as out (get out of the cluster, would trigger
    # rebalancing)
    NOOUT = "noout"
    # avoid marking osds as in (get in the cluster, would trigger rebalancing)
    NOIN = "noin"
    # avoid backfills (asynchronous recovery from journal log)
    NOBACKFILL = "nobackfill"
    # avoid rebalancing (data rebalancing will stop)
    NOREBALANCE = "norebalance"
    # avoid recovery (synchronous recovery of raw data)
    NORECOVER = "norecover"
    # avoid running any scrub job (independent from deep scrubs)
    NOSCRUB = "noscrub"
    # avoid running any deep scrub job
    NODEEP_SCRUB = "nodeep-scrub"
    # avoid cache tiering activity
    NOTIERAGENT = "notieragent"
    # avoid snapshot trimming (async deletion of objects from deleted
    # snapshots)
    NOSNAPTRIM = "nosnaptrim"
    # explitic hard limit the pg log (don't use, deprecated feature)
    PGLOG_HARDLIMIT = "pglog_hardlimit"


@dataclass(frozen=True)
class CephClusterStatus:
    """Status of a CEPH cluster."""

    status_dict: Dict[str, Any]

    def get_osdmap_set_flags(self) -> Set[CephOSDFlag]:
        """Get osdmap set flags."""
        osd_maps = self.status_dict["health"]["checks"].get("OSDMAP_FLAGS")
        if not osd_maps:
            return []

        raw_flags_line = osd_maps["summary"]["message"]
        if "flag(s) set" not in raw_flags_line:
            return []

        # ex: "noout,norebalance flag(s) set"
        flags = raw_flags_line.split(" ")[0].split(",")
        return set(CephOSDFlag(flag) for flag in flags)

    @staticmethod
    def _filter_out_octopus_upgrade_warns(status: Dict[str, Any]) -> Dict[str, Any]:
        # ignore temporary alert for octopus upgrade
        # https://docs.ceph.com/en/latest/security/CVE-2021-20288/#recommendations
        new_status = deepcopy(status)
        there_were_health_checks = bool(len(new_status["health"]["checks"]) > 0)

        if "AUTH_INSECURE_GLOBAL_ID_RECLAIM" in new_status["health"]["checks"]:
            del new_status["health"]["checks"]["AUTH_INSECURE_GLOBAL_ID_RECLAIM"]

        if "AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED" in new_status["health"]["checks"]:
            del new_status["health"]["checks"]["AUTH_INSECURE_GLOBAL_ID_RECLAIM_ALLOWED"]

        # if there were no health checks to start with, something was very wrong in the cluster.
        if there_were_health_checks and len(new_status["health"]["checks"]) == 0:
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
            return current_flags.issubset({CephOSDFlag("noout"), CephOSDFlag("norebalance")})

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

    def get_in_progress(self) -> None:
        """Get the current in-progress events."""
        return self.status_dict.get("progress_events", {})


class CephOSDController:
    """Controller for a CEPH node."""

    SYSTEM_DEVICES = ["sda", "sdb"]

    def __init__(self, remote: Remote, node_fqdn: str):
        """Init."""
        self._remote = remote
        self._node_fqdn = node_fqdn
        self._node = self._remote.query(f"D{{{self._node_fqdn}}}", use_sudo=True)

    @classmethod
    def _is_device_available(cls, device_info: Dict[str, Any]) -> bool:
        return (
            device_info["name"] not in cls.SYSTEM_DEVICES
            and not device_info.get("children")
            and device_info.get("type") == "disk"
            and not device_info.get("mountpoint")
        )

    def get_available_devices(self) -> List[str]:
        """Get the current available devices in the node."""
        structured_output = run_one(command=["lsblk", "--json"], node=self._node)
        if "blockdevices" not in structured_output:
            raise CephMalformedInfo(
                f"Missing 'blockdevices' on lsblk output: {json.dumps(structured_output, indent=4)}"
            )

        return [
            f"/dev/{device_info['name']}"
            for device_info in structured_output["blockdevices"]
            if self._is_device_available(device_info=device_info)
        ]

    def zap_device(self, device_path: str) -> None:
        """Zap the given device.

        NOTE: this destroys all the information in the device!
        """
        self._node.run_sync(f"ceph-volume lvm zap {device_path}")

    def initialize_and_start_osd(self, device_path: str) -> None:
        """Setup and start a new osd on the given device."""
        self._node.run_sync(f"ceph-volume lvm create --bluestore --data {device_path}")

    def add_all_available_devices(self, interactive: bool = True) -> None:
        """Discover and add all the available devices of the node as new OSDs."""
        for device_path in self.get_available_devices():
            if interactive:
                ask_confirmation(f"I'm going to destroy and create a new OSD on {self._node_fqdn}:{device_path}.")

            self.zap_device(device_path=device_path)
            self.initialize_and_start_osd(device_path=device_path)


class CephClusterController:
    """Controller for a CEPH cluster."""

    def __init__(self, remote: Remote, controlling_node_fqdn: str):
        """Init."""
        self._remote = remote
        self._controlling_node_fqdn = controlling_node_fqdn
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)

    def get_nodes(self) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        # There's usually a couple empty lines before the json data
        return run_one(command=["ceph", "node", "ls", "-f", "json"], node=self._controlling_node, last_line_only=True)

    def get_nodes_domain(self) -> str:
        """Get the network domain for the nodes in the cluster."""
        return self._controlling_node_fqdn.split(".", 1)[-1]

    def change_controlling_node(self) -> None:
        """Change the current node being used to interact with the cluster for another one."""
        current_monitor_name = self._controlling_node_fqdn.split(".", 1)[0]
        nodes = self.get_nodes()
        try:
            another_monitor = next(node_host for node_host in nodes["mon"].keys() if node_host != current_monitor_name)
        except StopIteration as error:
            raise CephNoControllerNode(
                f"Unable to find any other mon node to control the cluster, got nodes: {nodes}"
            ) from error

        self._controlling_node_fqdn = f"{another_monitor}.{self.get_nodes_domain()}"
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)
        LOGGER.info("Changed to node %s to control the CEPH cluster.", self._controlling_node_fqdn)

    def get_cluster_status(self) -> CephClusterStatus:
        """Get the current cluster status."""
        cluster_status_output = run_one(command=["ceph", "status", "-f", "json"], node=self._controlling_node)
        return CephClusterStatus(status_dict=cluster_status_output)

    def set_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Set one of the osdmap flags."""
        set_osdmap_flag_result = run_one(command=["ceph", "osd", "set", flag.value], node=self._controlling_node)
        if set_osdmap_flag_result != f"{flag.value} is set":
            raise CephFlagSetError(f"Unable to set `{flag.value}` on the cluster, got output: {set_osdmap_flag_result}")

    def unset_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Unset one of the osdmap flags."""
        unset_osdmap_flag_result = run_one(command=["ceph", "osd", "unset", flag.value], node=self._controlling_node)
        if unset_osdmap_flag_result != f"{flag.value} is unset":
            raise CephFlagSetError(
                f"Unable to unset `{flag.value}` on the cluster, got output: {unset_osdmap_flag_result}"
            )

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

        self.set_osdmap_flag(flag=CephOSDFlag("noout"))
        self.set_osdmap_flag(flag=CephOSDFlag("norebalance"))

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
            self.unset_osdmap_flag(flag=CephOSDFlag("noout"))
            self.unset_osdmap_flag(flag=CephOSDFlag("norebalance"))

        else:
            LOGGER.info("Cluster already out of maintenance status.")

    def wait_for_in_progress_events(self, timeout_seconds: int = 600) -> None:
        """Wait until a cluster in progress events have finished."""
        check_interval_seconds = 10
        start_time = time.time()
        cur_time = start_time
        while cur_time - start_time < timeout_seconds:
            cluster_status = self.get_cluster_status()
            in_progress_events = cluster_status.get_in_progress()
            if not in_progress_events:
                return

            mean_progress = (
                sum(event["progress"] for event in in_progress_events.values()) * 100 / len(in_progress_events)
            )
            LOGGER.info(
                "Cluster still has (%d) in-progress events, %.2f%% done, waiting another %d (timeout=%d)...",
                len(in_progress_events),
                mean_progress,
                check_interval_seconds,
                timeout_seconds,
            )

            time.sleep(check_interval_seconds)
            cur_time = time.time()

        raise CephTimeout(
            f"Waited {timeout_seconds} for the cluster to finish in-progress events, but it never did, current state:\n"
            f"\n{json.dumps(cluster_status.get_in_progress(), indent=4)}"
        )

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

        return cls(master_url=master_url, dns_url=dns_url, metrics_url=metrics_url)


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
        raw_output = run_one(
            # cluster-info does not support json output format (there's a dump
            # command, but it's too verbose)
            command=["kubectl", "custer-info"],
            node=self._controlling_node,
        )
        return KubernetesClusterInfo.form_cluster_info_output(raw_output=raw_output)

    def get_nodes(self, selector: Optional[str] = None) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        if selector:
            selector_cli = f"--selector='{selector}'"
        else:
            selector_cli = ""

        output = run_one(
            command=["kubectl", "get", "nodes", "--output=json", selector_cli], node=self._controlling_node
        )
        return output["items"]

    def get_node(self, node_hostname: str) -> Dict[str, Any]:
        """Get only info for the the given node."""
        return self.get_nodes(selector=f"kubernetes.io/hostname={node_hostname}")

    def get_pods(self, field_selector: Optional[str] = None) -> Dict[str, Any]:
        """Get pods."""
        if field_selector:
            field_selector_cli = f"--field-selector='{field_selector}'"
        else:
            field_selector_cli = ""

        output = run_one(
            command=["kubectl", "get", "pods", "--output=json", field_selector_cli], node=self._controlling_node
        )
        return output["items"]

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
        except StopIteration as error:
            raise KubernetesNodeStatusError(
                f"Unable to get 'Ready' condition of node {node_hostname}, got conditions:\n"
                f"{node_info[0]['conditions']}"
            ) from error


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
        raw_output = run_one(command=["kubeadm", "token", "create"], node=self._controlling_node)
        output = raw_output.splitlines()[-1].strip()
        if not output:
            raise KubeadmCreateTokenError(f"Error creating a new token:\nOutput:{raw_output}")

        return output

    def delete_token(self, token: str) -> str:
        """Removes the given bootstrap token."""
        raw_output = run_one(command=["kubeadm", "token", "delete", token], node=self._controlling_node)
        if "deleted" not in raw_output:
            raise KubeadmDeleteTokenError(f"Error deleting token {token}:\nOutput:{raw_output}")

        return raw_output.strip()

    def get_ca_cert_hash(self) -> str:
        """Retrieves the CA cert hash to use when bootstrapping."""
        raw_output = run_one(
            command=[
                "openssl x509 -pubkey -in /etc/kubernetes/pki/ca.crt",
                "| openssl rsa -pubin -outform der 2>/dev/null",
                "| openssl dgst -sha256 -hex",
                "| sed 's/^.* //'",
            ],
            node=self._controlling_node,
        )
        return raw_output.strip()

    def join(
        self, kubernetes_controller: KubernetesController, wait_for_ready: bool = True, timeout_seconds: int = 600
    ) -> None:
        """Join this node to the kubernetes cluster controlled by the given controller."""
        control_kubeadm = KubeadmController(
            remote=self._remote, controlling_node_fqdn=kubernetes_controller.controlling_node_fqdn
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


def run_one(
    command: Union[List[str], Command],
    node: RemoteHosts,
    capture_errors: bool = False,
    last_line_only: bool = False,
    try_format: OutputFormat = OutputFormat.JSON,
    **kwargs,
) -> Union[Dict[str, Any], str]:
    """Run a command on a node.

    Returns the loaded json if able, otherwise the raw output.

    Any extra kwargs will be passed to the RemoteHosts.run_sync function.
    """
    if not isinstance(command, Command):
        command = Command(command=" ".join(command), ok_codes=[0, 1, 2] if capture_errors else [0])

    try:
        result = next(node.run_sync(command, **kwargs))

    except StopIteration:
        result = None

    if result is None:
        raw_result = "{}"
    else:
        raw_result = result[1].message().decode()
        if last_line_only:
            raw_result = raw_result.splitlines()[-1]

    try:
        if try_format == OutputFormat.JSON:
            return json.loads(raw_result)

        if try_format == OutputFormat.YAML:
            return yaml.safe_load(raw_result)

    except (json.JSONDecodeError, yaml.YAMLError):
        pass

    return raw_result


class GridError(Exception):
    """Base parent class for all grid related exceptions."""


class GridNodeNotFound(GridError):
    """Risen when a node was not found in the cluster."""


class GridUnableToJoin(GridError):
    """Risen when a node was unable to join a cluster."""


class GridQueueType(Enum):
    """Enum representing all grid queue types."""

    BATCH = "B"
    INTERACTIVE = "I"
    CHECKPOINTING = "C"
    PARALLEL = "P"
    NONE = "N"


@dataclass(frozen=True)
class GridQueueTypesSet:
    """Class representing a grid queue types set."""

    types: List[GridQueueType]

    @classmethod
    def from_types_string(cls, types_string: Optional[str]) -> "GridQueueTypesSet":
        """Create a GridQueueStatesSet from qhost queue types string."""
        if not types_string:
            return []

        return cls(types=[GridQueueType(type_char) for type_char in types_string])


class GridQueueState(Enum):
    """Enum representing all grid queue states."""

    OK = "_"  # virtual state, if there is no state information, the queue is OK

    UNKNOWN = "u"
    ALARM1 = "a"
    ALARM2 = "A"
    CALENDAR_SUSPENDED = "C"
    SUSPENDED = "s"
    SUBORDINATE = "S"
    DISABLED1 = "d"
    DISABLED2 = "D"
    ERROR = "E"
    CONFIGURATION_AMBIGUOUS = "c"
    ORPHANED = "o"
    PREEMPTED = "P"


@dataclass(frozen=True)
class GridQueueStatesSet:
    """Class that contains all the data associated to a grid queue status set."""

    states: List[GridQueueState]

    @classmethod
    def from_state_string(cls, state_string: Optional[str]) -> "GridQueueStatesSet":
        """Create a GridQueueStatesSet from qhost queue state string."""
        if not state_string:
            # if the XML contains no state info, use this virtual state to indicate is OK
            state_string = GridQueueState.OK.value

        return cls(states=[GridQueueState(state_char) for state_char in state_string])

    def is_ok(self):
        """Return if this state set is a 'running' state set."""
        return (
            GridQueueState.ALARM1 not in self.states
            and GridQueueState.ALARM2 not in self.states
            and GridQueueState.ERROR not in self.states
        )


@dataclass(frozen=True)
class GridQueueInfo:
    """Class that contains all the data associated to a grid queue."""

    name: str
    types: Optional[str] = None
    slots_used: Optional[int] = None
    slots: Optional[int] = None
    slots_resv: Optional[int] = None
    statuses: Optional[GridQueueStatesSet] = None

    @classmethod
    def from_xml(cls, xml_obj: ElementTree) -> "GridQueueInfo":
        """Create a GridQueueInfo from qhost xml output queue node."""
        info_params = {"name": xml_obj.attrib.get("name")}
        for queuevalue_xml in xml_obj.iter("queuevalue"):
            value_type = queuevalue_xml.attrib.get("name")
            if value_type == "state_string":
                info_params["statuses"] = GridQueueStatesSet.from_state_string(state_string=queuevalue_xml.text)
            elif value_type == "qtype_string":
                info_params["types"] = GridQueueTypesSet.from_types_string(types_string=queuevalue_xml.text)
            else:
                info_params[value_type] = queuevalue_xml.text if queuevalue_xml.text != "-" else None

        return cls(**info_params)

    def is_ok(self):
        """Return if this queue is in a 'running' state."""
        return self.statuses.is_ok()


@dataclass(frozen=True)
class GridNodeInfo:
    """Class that contains all the data associated to a grid node."""

    name: str
    queues_info: Dict[str, GridQueueInfo]
    arch_string: Optional[str] = None
    num_proc: Optional[int] = None
    m_socket: Optional[int] = None
    m_core: Optional[int] = None
    m_thread: Optional[int] = None
    load_avg: Optional[float] = None
    mem_total: Optional[float] = None
    mem_used: Optional[float] = None
    swap_total: Optional[float] = None
    swap_used: Optional[float] = None

    @classmethod
    def from_xml(cls, xml_obj: ElementTree) -> "GridNodeInfo":
        """Create a GridNodeInfo from qhost xml output."""
        info_params = {"name": xml_obj.attrib.get("name"), "queues_info": {}}
        for hostvalue_xml in xml_obj.iter("hostvalue"):
            value_type = hostvalue_xml.attrib.get("name")
            info_params[value_type] = hostvalue_xml.text if hostvalue_xml.text != "-" else None

        for queue_xml in xml_obj.iter("queue"):
            queue_info = GridQueueInfo.from_xml(xml_obj=queue_xml)
            info_params["queues_info"][queue_info.name] = queue_info

        return cls(**info_params)

    def is_ok(self) -> bool:
        """Return if the node is in a 'running' status on all it's queues."""
        return all(queue.is_ok() for queue in self.queues_info.values())


class GridController:
    """Grid cluster controller class."""

    def __init__(self, remote: Remote, master_node_fqdn: str):
        """Init."""
        self._remote = remote
        self._master_node_fqdn = master_node_fqdn
        self._master_node = self._remote.query(f"D{{{self._master_node_fqdn}}}", use_sudo=True)

    def reconfigure(self, is_tools_project: bool) -> None:
        """Runs puppet and `grid-configurator --all-domains` on the grid master node."""
        # in most cases, the grid master needs to run puppet so collectors are up-to-date
        # otherwise the grid-configurator call may run over an incomplete environment
        PuppetHosts(remote_hosts=self._master_node).run(timeout=60)

        extra_param = "--beta" if not is_tools_project else ""
        self._master_node.run_sync(f"grid-configurator --all-domains {extra_param}")

    def add_node(self, host_fqdn: str, is_tools_project: bool, force: bool = False) -> None:
        """Adds a node to the cluster this controller's master node is part of."""
        if not force:
            try:
                node_info = self.get_node_info(host_fqdn=host_fqdn)
                if node_info.queues_info and node_info.is_ok():
                    LOGGER.info(
                        "Node %s was already part of this grid cluster and is running correctly, current status:\n%s",
                        host_fqdn,
                        str(node_info),
                    )
                else:
                    LOGGER.info(
                        (
                            "Node %s was already part of this grid cluster but it seems it's not properly setup, you "
                            "can rerun with --force to try adding it again, current status:\n%s"
                        ),
                        host_fqdn,
                        str(node_info),
                    )
                return

            except GridNodeNotFound:
                pass

        new_node = self._remote.query(f"D{{{host_fqdn}}}", use_sudo=True)

        LOGGER.info(
            "Refreshing configuration on grid master %s a couple times, and giving it 5 seconds.",
            self._master_node_fqdn,
        )
        self.reconfigure(is_tools_project)
        self.reconfigure(is_tools_project)
        time.sleep(5)

        LOGGER.info("Fake-starting gridengine-exec on the node %s, this is expected to fail", host_fqdn)
        new_node.run_sync(Command(command="systemctl start gridengine-exec", ok_codes=[]))

        LOGGER.info("Restarting gridengine master to pick up the changes on host_aliases file, and giving it 5 seconds")
        self._master_node.run_sync("systemctl stop gridengine-master.service")
        self._master_node.run_sync("systemctl start gridengine-master.service")
        time.sleep(5)

        LOGGER.info("For-real-restarting gridengine-exec on the node %s, this should not fail", host_fqdn)
        new_node.run_sync(Command(command="systemctl stop gridengine-exec", ok_codes=[]))
        new_node.run_sync(Command(command="systemctl start gridengine-exec"))

        try:
            node_info = self.get_node_info(host_fqdn=host_fqdn)
            if node_info.queues_info and node_info.is_ok():
                LOGGER.info(
                    "Node %s was correctly added to the grid cluster managed by %s, current status:\n%s",
                    host_fqdn,
                    self._master_node_fqdn,
                    str(node_info),
                )
                return

            # else:
            raise GridUnableToJoin(
                f"Node {host_fqdn} joined the cluster {self._master_node_fqdn} but it's in an error/not ok state, "
                "you can try rerunning with '--force' to try again, but might require manual intervention. Currest "
                f"status: {node_info}"
            )

        except GridNodeNotFound as error:
            LOGGER.error()
            raise GridUnableToJoin(
                f"Node {host_fqdn} did not join the cluster {self._master_node_fqdn}, you can try rerunning with "
                "'--force' to try again, but might require manual intervention."
            ) from error

    def get_nodes_info(self) -> Dict[str, GridNodeInfo]:
        """Retrieve node and queue information from the nodes currently in the cluster."""
        nodes_info: Dict[str, GridNodeInfo] = {}

        xml_output: str = next(self._master_node.run_sync("qhost -q -xml", print_output=False))[1].message().decode()
        parsed_xml = ElementTree.fromstring(xml_output)
        for node_xml in parsed_xml:
            if node_xml.tag == "global":
                continue
            node_info = GridNodeInfo.from_xml(xml_obj=node_xml)
            nodes_info[node_info.name] = node_info

        return nodes_info

    def get_node_info(self, host_fqdn: str) -> GridNodeInfo:
        """Retrieve node and queue information from the given node.

        Raises:
            GridNodeNotFound: when the node is not found in the cluster

        """
        xml_output = (
            next(self._master_node.run_sync(f"qhost -q -xml -h {host_fqdn}", print_output=False))[1].message().decode()
        )
        parsed_xml = ElementTree.fromstring(xml_output)
        for node_xml in parsed_xml:
            if node_xml.attrib["name"] == "global":
                continue

            return GridNodeInfo.from_xml(xml_obj=node_xml)

        raise GridNodeNotFound(f"Unable to find node {host_fqdn}, output:\n{xml_output}")

    def depool_node(self, host_fqdn: str) -> None:
        """Depools a node from the grid.

        Raises:
            GridNodeNotFound: when the node is not found in the cluster

        """
        # call this just to report upstream an exception
        self.get_node_info(host_fqdn)
        hostname = host_fqdn.split(".")[0]
        self._master_node.run_sync(f"exec-manage depool {hostname}", print_output=False)


def simple_create_file(dst_node: RemoteHosts, contents: str, remote_path: str, use_root: bool = True) -> None:
    """Creates a file on the remote host/hosts with the given content."""
    # this makes it esier to get away with quotes or similar
    base64_content = base64.b64encode(contents.encode("utf8"))
    full_command = ["echo", f"'{base64_content.decode()}'", "|", "base64", "--decode", "|"]
    if use_root:
        full_command.extend(["sudo", "-i"])

    full_command.extend(["tee", remote_path])

    return run_one(node=dst_node, command=full_command)


def natural_sort_key(element: str) -> List[Union[str, int]]:
    """Changes "name-12.something.com" into ["name-", 12, ".something.com"]."""
    return [int(mychunk) if mychunk.isdigit() else mychunk for mychunk in DIGIT_RE.split(element)]


def wrap_with_sudo_icinga(my_spicerack: Spicerack) -> Spicerack:
    """Wrap spicerack icinga to allow sudo.

    We have to patch the master host to allow sudo, all this weirdness is
    because icinga_master_host is a @property and can't be patched on
    the original instance
    """

    class SudoIcingaSpicerackWrapper(Spicerack):
        """Dummy wrapper class to allow sudo icinga."""

        def __init__(self):  # pylint: disable-msg=super-init-not-called
            """Init."""

        @property
        def icinga_master_host(self) -> RemoteHosts:
            """Icinga master host."""
            new_host = self.remote().query(query_string=self.dns().resolve_cname(ICINGA_DOMAIN), use_sudo=True)
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


# Poor man's namespace to compensate for the restriction to not create modules
@dataclass(frozen=True)
class TestUtils:
    """Generic testing utilities."""

    @staticmethod
    def to_parametrize(test_cases: Dict[str, Dict[str, Any]]) -> Dict[str, Union[str, List[Any]]]:
        """Helper for parametrized tests.

        Use like:
        @pytest.mark.parametrize(**_to_parametrize(
            {
                "Test case 1": {"param1": "value1", "param2": "value2"},
                # will set the value of the missing params as `None`
                "Test case 2": {"param1": "value1"},
                ...
            }
        ))
        """
        _param_names = set(chain(*[list(params.keys()) for params in test_cases.values()]))

        def _fill_up_params(test_case_params):
            # {
            #    'key': value,
            #    'key2': value2,
            # }
            end_params = []
            for must_param in _param_names:
                end_params.append(test_case_params.get(must_param, None))

            return end_params

        if len(_param_names) == 1:
            argvalues = [_fill_up_params(test_case_params)[0] for test_case_params in test_cases.values()]

        else:
            argvalues = [_fill_up_params(test_case_params) for test_case_params in test_cases.values()]

        return {"argnames": ",".join(_param_names), "argvalues": argvalues, "ids": list(test_cases.keys())}

    @staticmethod
    def get_fake_remote(responses: List[str] = None, side_effect: Optional[List[Any]] = None) -> mock.MagicMock:
        """Create a fake remote.

        It will return a RemoteHosts that will return the given responses when run_sync is called in them.
        If side_effect is passed, it will override the responses and set that as side_effect of the mock on run_sync.
        """
        fake_hosts = mock.create_autospec(spec=RemoteHosts, spec_set=True)
        fake_remote = mock.create_autospec(spec=Remote, spec_set=True)

        fake_remote.query.return_value = fake_hosts

        def _get_fake_msg_tree(response: str):
            fake_msg_tree = mock.create_autospec(spec=MsgTreeElem, spec_set=True)
            fake_msg_tree.message.return_value = response.encode()
            return fake_msg_tree

        if side_effect is not None:
            fake_hosts.run_sync.side_effect = side_effect
        else:
            # the return type of run_sync is Iterator[Tuple[NodeSet, MsgTreeElem]]
            fake_hosts.run_sync.return_value = ((None, _get_fake_msg_tree(response=response)) for response in responses)

        return fake_remote


# Poor man's namespace to compensate for the restriction to not create modules
@dataclass(frozen=True)
class CephTestUtils(TestUtils):
    """Utils to test ceph related code."""

    @staticmethod
    def get_status_dict(overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate a stub status dict to use when creating CephStatus"""
        status_dict = {"health": {"status": {}, "checks": {}}}

        def _merge_dict(to_update, source_dict):
            if not source_dict:
                return

            for key, value in source_dict.items():
                if key in to_update and isinstance(value, dict):
                    _merge_dict(to_update[key], value)
                else:
                    to_update[key] = value

        _merge_dict(to_update=status_dict, source_dict=overrides)
        return status_dict

    @classmethod
    def get_maintenance_status_dict(cls):
        """Generate a stub maintenance status dict to use when creating CephStatus"""
        maintenance_status_dict = {
            "health": {
                "status": "HEALTH_WARN",
                "checks": {"OSDMAP_FLAGS": {"summary": {"message": "noout,norebalance flag(s) set"}}},
            }
        }

        return cls.get_status_dict(maintenance_status_dict)

    @classmethod
    def get_ok_status_dict(cls):
        """Generate a stub maintenance status dict to use when creating CephStatus"""
        ok_status_dict = {"health": {"status": "HEALTH_OK"}}

        return cls.get_status_dict(ok_status_dict)

    @classmethod
    def get_warn_status_dict(cls):
        """Generate a stub maintenance status dict to use when creating CephStatus"""
        warn_status_dict = {"health": {"status": "HEALTH_WARN"}}

        return cls.get_status_dict(warn_status_dict)

    @staticmethod
    def get_available_device(
        name: str = f"{CephOSDController.SYSTEM_DEVICES[0]}_non_matching_part",
        device_type: str = "disk",
        children: Optional[List[Any]] = None,
        mountpoint: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Get a device that is considered available.

        If you pass any value, it will not ensure that it's still considered available.
        """
        available_device = {"name": name, "type": device_type}
        if children is not None:
            available_device["children"] = children

        if mountpoint is not None:
            available_device["mountpoint"] = mountpoint

        return available_device
