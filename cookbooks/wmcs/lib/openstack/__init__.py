#!/usr/bin/env python3
# pylint: disable=too-many-arguments
"""Openstack generic related code."""
import logging
import re
import time
from enum import Enum, auto
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Type, Union

import yaml
from cumin.transports import Command
from spicerack.remote import Remote, RemoteHosts

from cookbooks.wmcs import (
    OutputFormat,
    run_one_as_dict,
    run_one_formatted,
    run_one_formatted_as_list,
    run_one_raw,
    simple_create_file,
)

LOGGER = logging.getLogger(__name__)
AGGREGATES_FILE_PATH = "/etc/wmcs_host_aggregates.yaml"
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


class Deployment(Enum):
    """Deployment enumerate"""

    EQIAD1 = "eqiad1"
    CODFW1DEV = "codfw1dev"

    def __str__(self):
        """String representation"""
        return self.value

    @classmethod
    def get_deployment_for_node(cls, node: str) -> "Deployment":
        """Retrieve the deployment given a node fqdn/name.

        This tries several strategies in priority order:
        * Check the known deployments (see the get_*_nodes functions)
        * Check the hosts domain name (<deployment>.wmnet)
        * Check the host name (<name>YXXX.<domain>, where Y symbolizes the deployment)
        """
        for deployment in list(Deployment):
            for node_group in _OPENSTACK_NODES[deployment].values():
                if node in node_group:
                    return deployment

        if node.count(".") >= 2:
            domain = node.rsplit(".", 2)[1]
            try:
                return cls(domain)
            except ValueError:
                pass

        deploy_match = re.match(r"[^.]*(?<deployment_number>\d)+", node)
        if deploy_match:
            if deploy_match.groupdict()["deployment_number"] == 1:
                return cls.EQIAD1
            if deploy_match.groupdict()["deployment_number"] == 2:
                return cls.CODFW1DEV

        raise Exception(f"Unable to guess deployment for node {node}")


# Use FQDNs here
_OPENSTACK_NODES = {
    Deployment.EQIAD1: {
        "gateway-nodes": [
            "cloudgw1001.eqiad.wmnet",
            "cloudgw1002.eqiad.wmnet",
        ],
        "control-nodes": [
            "cloudcontrol1003.wikimedia.org",
            "cloudcontrol1004.wikimedia.org",
            "cloudcontrol1005.wikimedia.org",
        ],
    },
    Deployment.CODFW1DEV: {
        "gateway-nodes": [
            "cloudgw2001-dev.codfw.wmnet",
            "cloudgw2002-dev.codfw.wmnet",
            "cloudgw2003-dev.codfw.wmnet",
        ],
        "control-nodes": [
            "cloudcontrol2001-dev.wikimedia.org",
            "cloudcontrol2003-dev.wikimedia.org",
            "cloudcontrol2004-dev.wikimedia.org",
        ],
    },
}


OpenstackID = str
OpenstackName = str
OpenstackIdentifier = Union[OpenstackID, OpenstackName]


def get_control_nodes(deployment: Deployment) -> List[str]:
    """Get all the FQDNs of the control nodes (in the future with netbox or similar)."""
    return _OPENSTACK_NODES[deployment]["control-nodes"]


def get_gateway_nodes(deployment: Deployment) -> List[str]:
    """Get all the FQDNs of the gateway nodes (in the future with netbox or similar)."""
    return _OPENSTACK_NODES[deployment]["gateway-nodes"]


def _quote(mystr: str) -> str:
    """Wraps the given string in single quotes."""
    return f"'{mystr}'"


def wait_for_it(
    condition_fn: Callable[..., bool],
    condition_name_msg: str,
    when_failed_raise_exception: Type[Exception],
    condition_failed_msg_fn: Callable[..., str],
    timeout_seconds: int = 900,
):
    """Wait until a condition happens.

    It will call the callable until it returns True, or timeout_seconds passed, in which case it will raise
    when_failed_raise_exception with the return value of condition_failed_msg_fn.
    """
    check_interval_seconds = 10
    start_time = time.time()
    cur_time = start_time
    while cur_time - start_time < timeout_seconds:
        if condition_fn():
            return

        LOGGER.info(
            "'%s' failed, waiting another %ds (timeout=%ds, %ds elapsed)...",
            condition_name_msg,
            check_interval_seconds,
            timeout_seconds,
            cur_time - start_time,
        )

        time.sleep(check_interval_seconds)
        cur_time = time.time()

    raise when_failed_raise_exception(
        f"Waited {timeout_seconds} for {condition_name_msg}, but it never happened:\n" f"{condition_failed_msg_fn()}"
    )


class CommandRunnerMixin:
    """Mixin to get command running functions."""

    def __init__(self, command_runner_node: RemoteHosts):
        """Simple mixin to provide command running functions to a class."""
        self.command_runner_node = command_runner_node

    def _get_full_command(self, *command: str, json_output: bool = True):
        raise NotImplementedError

    def run_raw(
        self, *command: str, is_safe: bool = False, capture_errors: bool = False, json_output=True, **kwargs
    ) -> str:
        """Run an openstack command on a control node.

        Returns the raw output (not loaded from json).

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        full_command = self._get_full_command(*command, json_output=json_output)
        return run_one_raw(
            command=full_command,
            node=self.command_runner_node,
            is_safe=is_safe,
            capture_errors=capture_errors,
            **kwargs,
        )

    def run_formatted_as_dict(
        self, *command: str, is_safe: bool = False, capture_errors: bool = False, **kwargs
    ) -> Dict[str, Any]:
        """Run an openstack command on a control node forcing json output.

        Returns a dict with the formatted output (loaded from json), usually for show commands.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.

        Example:
            >>> self.run_formatted("port", "show")
            {
                "admin_state_up": true,
                "allowed_address_pairs": [],
                ...
                "status": "ACTIVE",
                "tags": [],
                "trunk_details": null,
                "updated_at": "2022-04-21T05:18:43Z"
            }

        """
        full_command = self._get_full_command(*command, json_output=True)
        return run_one_as_dict(
            command=full_command,
            node=self.command_runner_node,
            is_safe=is_safe,
            capture_errors=capture_errors,
            **kwargs,
        )

    def run_formatted_as_list(
        self, *command: str, is_safe: bool = False, capture_errors: bool = False, **kwargs
    ) -> List[Any]:
        """Run an openstack command on a control node forcing json output.

        Returns a list with the formatted output (loaded from json), usually for `list` commands.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.

        Example:
            >>> self.run_formatted_as_list("port", "list")
            [
                {
                    "ID": "fb751dd4-05bb-4f23-822f-852f55591a11",
                    "Name": "",
                    "MAC Address": "fa:16:3e:25:48:ca",
                    "Fixed IP Addresses": [
                        {
                            "subnet_id": "7adfcebe-b3d0-4315-92fe-e8365cc80668",
                            "ip_address": "172.16.128.110"
                        }
                    ],
                    "Status": "ACTIVE"
                },
                {
                    "ID": "fb9a2e11-39af-4fa2-80a7-5f895d42b68a",
                    "Name": "",
                    "MAC Address": "fa:16:3e:7f:80:e8",
                    "Fixed IP Addresses": [
                        {
                            "subnet_id": "7adfcebe-b3d0-4315-92fe-e8365cc80668",
                            "ip_address": "172.16.128.115"
                        }
                    ],
                    "Status": "DOWN"
                },
            ]

        """
        full_command = self._get_full_command(*command, json_output=True)
        return run_one_formatted_as_list(
            command=full_command,
            node=self.command_runner_node,
            is_safe=is_safe,
            capture_errors=capture_errors,
            **kwargs,
        )


class OpenstackError(Exception):
    """Parent class for all openstack related errors."""


class OpenstackNotFound(OpenstackError):
    """Thrown when trying to get an element from Openstack gets no results."""


class OpenstackMigrationError(OpenstackError):
    """Thrown when there's an issue with migration."""


class OpenstackBadQuota(OpenstackError):
    """Thrown when the quota given is not known or incorrect."""


class OpenstackRuleDirection(Enum):
    """Direction for the security group rule."""

    INGRESS = auto()
    EGRESS = auto()


class OpenstackQuotaName(Enum):
    """Known quota names"""

    BACKUP_GIGABYTES = "backup-gigabytes"
    BACKUPS = "backups"
    CORES = "cores"
    FIXED_IPS = "fixed-ips"
    FLOATING_IPS = "floating-ips"
    GIGABYTES = "gigabytes"
    GIGABYTES_STANDARD = "gigabytes_standard"
    GROUPS = "groups"
    INJECTED_FILE_SIZE = "injected-file-size"
    INJECTED_FILES = "injected-files"
    INJECTED_PATH_SIZE = "injected-path-size"
    INSTANCES = "instances"
    KEY_PAIRS = "key-pairs"
    NETWORKS = "networks"
    PER_VOLUME_GIGABYTES = "per-volume-gigabytes"
    PORTS = "ports"
    PROPERTIES = "properties"
    RAM = "ram"
    RBAC_POLICIES = "rbac_policies"
    ROUTERS = "routers"
    SECGROUP_RULES = "secgroup-rules"
    SECGROUPS = "secgroups"
    SERVER_GROUP_MEMBERS = "server-group-members"
    SERVER_GROUPS = "server-groups"
    SNAPSHOTS = "snapshots"
    SNAPSHOTS_STANDARD = "snapshots_standard"
    SUBNET_POOLS = "subnet_pools"
    SUBNETS = "subnets"
    VOLUMES = "volumes"
    VOLUMES_STANDARD = "volumes_standard"


class Unit(Enum):
    """Basic information storage units."""

    GIGA = "G"
    MEGA = "M"
    KILO = "K"
    UNIT = "B"

    def next_unit(self) -> "Unit":
        """Decreases the given unit by one order of magnitude."""
        if self == Unit.GIGA:
            return Unit.MEGA
        if self == Unit.MEGA:
            return Unit.KILO
        if self == Unit.KILO:
            return Unit.UNIT

        raise OpenstackBadQuota(f"Unit {self} can't be lowered.")


class OpenstackQuotaEntry(NamedTuple):
    """Represents a specific entry for a quota."""

    name: OpenstackQuotaName
    value: int

    def to_cli(self) -> str:
        """Return the openstack cli equivalent of setting this quota entry."""
        return f"--{self.name.value.lower().replace('_', '-')}={self.value}"

    @classmethod
    def from_human_spec(cls, name: OpenstackQuotaName, human_spec: str) -> "OpenstackQuotaEntry":
        """Given a human spec (ex. 10G) and a quota name gives a quota entry with the right value."""
        return cls(
            name=name,
            value=cls._human_to_quota_number(
                human_spec=human_spec,
                quota_name=name,
            ),
        )

    @staticmethod
    def _human_to_quota_number(human_spec: str, quota_name: OpenstackQuotaName) -> int:
        """Maps from human strings (ex. 10G) to the string needed for the given quota.

        This is to be able to translate "add 10G of ram" to the number that openstack expects for the ram, that is
        megabytes.
        """
        if "gigabytes" in quota_name.value:
            dst_unit = Unit.GIGA
        elif quota_name == OpenstackQuotaName.RAM:
            dst_unit = Unit.MEGA
        else:
            dst_unit = Unit.UNIT

        try:
            int(human_spec[-1:])
            # expect that if no unit passed, it's using the one openstack expects
            cur_unit = dst_unit
            cur_value = int(human_spec)

        except ValueError:
            cur_unit = Unit(human_spec[-1:])
            cur_value = int(human_spec[:-1])

        while dst_unit != cur_unit:
            cur_value *= 1024
            try:
                cur_unit = cur_unit.next_unit()
            except OpenstackBadQuota as error:
                raise OpenstackBadQuota(
                    f"Unable to translate {human_spec} for {quota_name} (maybe the quota chosen does not support that "
                    "unit?)"
                ) from error

        return cur_value


class OpenstackServerGroupPolicy(Enum):
    """Affinity for the server group."""

    SOFT_ANTI_AFFINITY = "soft-anti-affinity"
    ANTI_AFFINITY = "anti-affinity"
    AFFINITY = "affinity"
    SOFT_AFFINITY = "soft-affinity"


class OpenstackAPI(CommandRunnerMixin):
    """Class to interact with the Openstack API (indirectly for now)."""

    def __init__(
        self, remote: Remote, control_node_fqdn: str = "cloudcontrol1003.wikimedia.org", project: OpenstackName = ""
    ):
        """Init."""
        self.project = project
        self.control_node_fqdn = control_node_fqdn
        self.control_node = remote.query(f"D{{{control_node_fqdn}}}", use_sudo=True)
        super().__init__(command_runner_node=self.control_node)

    def _get_full_command(self, *command: str, json_output: bool = True):
        # some commands don't have formatted output
        if json_output:
            format_args = ["-f", "json"]
        else:
            format_args = []
        if "delete" in command:
            format_args = []

        return ["env", f"OS_PROJECT_ID={self.project}", "wmcs-openstack", *command, *format_args]

    def get_nodes_domain(self) -> str:
        """Return the domain of the cluster handled by this controller.

        This is complicated as the cloudcontrols usually use the wikimedia.org domain.
        """
        eqiad_regex = r"[a-zA-Z]+1[0-9]+.*"
        codfw_regex = r"[a-zA-Z]+2[0-9]+.*"
        if re.match(eqiad_regex, self.control_node_fqdn):
            return "eqiad.wmnet"

        if re.match(codfw_regex, self.control_node_fqdn):
            return "codfw.wmnet"

        raise Exception(f"Unable to find node domain for {self.control_node_fqdn}")

    def create_service_ip(self, ip_name: OpenstackName, network: OpenstackIdentifier, **kwargs) -> Dict[str, Any]:
        """Create a service IP with a specified name

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_dict("port", "create", "--network", _quote(network), _quote(ip_name), **kwargs)

    def attach_service_ip(
        self, ip_address: str, server_port_id: OpenstackIdentifier, **kwargs
    ) -> Dict[OpenstackName, Any]:
        """Attach a specified service ip address to the specified port

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_dict(
            "port",
            "set",
            "--allowed-address",
            f"ip-address={ip_address}",
            _quote(server_port_id),
            json_output=False,
            **kwargs,
        )

    def detach_service_ip(
        self, ip_address: str, mac_addr: str, server_port_id: OpenstackIdentifier, **kwargs
    ) -> Dict[str, Any]:
        """Detach a specified service ip address from the specified port

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_dict(
            "port",
            "unset",
            "--allowed-address",
            f"ip-address={ip_address},mac-address={mac_addr}",
            _quote(server_port_id),
            json_output=False,
            **kwargs,
        )

    def port_get(self, ip_address, **kwargs) -> List[Dict[str, Any]]:
        """Get port for specified IP address"""
        ip_filter = f'--fixed-ip="ip-address={ip_address}"'
        return self.run_formatted_as_list("port", "list", ip_filter, **kwargs)

    def zone_get(self, name, **kwargs) -> List[Dict[str, Any]]:
        """Get zone record for specified dns zone"""
        return self.run_formatted_as_list("zone", "list", "--name", name, **kwargs)

    def recordset_create(self, zone_id, record_type, name, record, **kwargs) -> List[Dict[str, Any]]:
        """Get zone record for specified dns zone"""
        return self.run_formatted_as_list(
            "recordset", "create", "--type", record_type, "--record", record, zone_id, name, **kwargs
        )

    def server_show(self, vm_name: OpenstackIdentifier) -> Dict[str, Any]:
        """Get the information for a VM."""
        return self.run_formatted_as_dict("server", "show", vm_name, is_safe=True)

    def server_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Retrieve the list of servers for the project.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_list("server", "list", is_safe=True, **kwargs)

    def server_list_filter_exists(self, hostnames: List[str], **kwargs) -> List[str]:
        """Verify if all servers in the list exists.

        Returns the input list filtered with those hostnames that do exists.

        Any extra kwarg will be passed to the RemoteHosts.run_sync function.
        """
        listing = self.server_list(**kwargs)

        for hostname in hostnames:
            if not any(info for info in listing if info["Name"] == hostname):
                hostnames.remove(hostname)

        return hostnames

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

        Note that the name_to_remove is the name of the node as registered in
        Openstack, that's probably not the FQDN (and hopefully the hostname,
        but maybe not).
        """
        self.run_raw("server", "delete", name_to_remove, is_safe=False)

    def volume_create(self, name: OpenstackName, size: int) -> str:
        """Create a volume and return the ID of the created volume.

        --size is in GB
        """
        out = self.run_formatted_as_dict("volume", "create", "--size", str(size), "--type", "standard", name)
        return out["id"]

    def volume_attach(self, server_id: OpenstackID, volume_id: OpenstackID) -> None:
        """Attach a volume to a server"""
        self.run_raw("server", "add", "volume", server_id, volume_id, json_output=False)

    def volume_detach(self, server_id: OpenstackID, volume_id: OpenstackID) -> None:
        """Attach a volume to a server"""
        self.run_raw("server", "remove", "volume", server_id, volume_id, json_output=False)

    def server_from_id(self, server_id: OpenstackIdentifier) -> Dict[str, Any]:
        """Given the ID of a server, return the server details"""
        return self.run_formatted_as_dict("server", "show", server_id)

    def volume_from_id(self, volume_id: OpenstackIdentifier) -> Dict[str, Any]:
        """Given the ID of a volume, return the volume details"""
        return self.run_formatted_as_dict("volume", "show", volume_id)

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

        server_group_options = []
        if server_group_id:
            server_group_options.extend(["--hint", f"group={server_group_id}"])

        out = self.run_formatted_as_dict(
            "server",
            "create",
            "--flavor",
            _quote(flavor),
            "--image",
            _quote(image),
            "--network",
            _quote(network),
            "--wait",
            *server_group_options,
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
        server_aggregates: List[Dict[str, Any]] = []
        for aggregate in current_aggregates:
            aggregate_details = self.aggregate_show(
                aggregate=aggregate["Name"], print_output=False, print_progress_bars=False
            )
            if name in aggregate_details.get("hosts", []):
                server_aggregates.append(aggregate_details)

        return server_aggregates

    def security_group_list(self, **kwargs) -> List[Dict[str, Any]]:
        """Retrieve the list of security groups.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_list("security", "group", "list", is_safe=True, **kwargs)

    def security_group_create(self, name: OpenstackName, description: str) -> None:
        """Create a security group."""
        self.run_raw("security", "group", "create", name, "--description", _quote(description))

    def security_group_rule_create(
        self, direction: OpenstackRuleDirection, remote_group: OpenstackName, security_group: OpenstackName
    ) -> None:
        """Create a rule inside the given security group."""
        self.run_raw(
            "security",
            "group",
            "rule",
            "create",
            f"--{direction.name.lower}",
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
        return self.run_formatted_as_list("server", "group", "list", is_safe=True, **kwargs)

    def server_group_create(self, name: OpenstackName, policy: OpenstackServerGroupPolicy) -> None:
        """Create a server group."""
        self.run_raw(
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

        Raises OpenstackNotFound if there's no server group found with the given name.

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
        return self.run_formatted_as_list("aggregate", "list", "--long", is_safe=True, **kwargs)

    def aggregate_show(self, aggregate: OpenstackIdentifier, **kwargs) -> Dict[str, Any]:
        """Get the details of a given aggregate.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_dict("aggregate", "show", aggregate, is_safe=True, **kwargs)

    def aggregate_remove_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Remove the given host from the aggregate."""
        result = self.run_raw(
            "aggregate",
            "remove",
            "host",
            aggregate_name,
            host_name,
            capture_errors=True,
            print_output=False,
            print_progress_bars=False,
        )
        if "HTTP 404" in result:
            raise OpenstackNotFound(
                f"Node {host_name} was not found in aggregate {aggregate_name}, did you try using the hostname "
                "instead of the fqdn?"
            )

    def aggregate_add_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Add the given host to the aggregate."""
        result = self.run_raw("aggregate", "add", "host", aggregate_name, host_name, capture_errors=True)
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
    def aggregate_load_from_host(host: RemoteHosts) -> List[Dict[str, Any]]:
        """Load the persisted list of aggregates from the host."""
        try:
            result = run_one_formatted(
                command=["cat", AGGREGATES_FILE_PATH],
                node=host,
                is_safe=True,
                try_format=OutputFormat.YAML,
                print_output=False,
                print_progress_bars=False,
            )

        except Exception as error:
            raise OpenstackNotFound(f"Unable to cat the file {AGGREGATES_FILE_PATH} on host {host}") from error

        if isinstance(result, List):
            return result

        raise TypeError(f"Expected a list, got {result}")

    def drain_hypervisor(self, hypervisor_name: OpenstackName) -> None:
        """Drain a hypervisor."""
        command = Command(
            command=f"bash -c 'source /root/novaenv.sh && wmcs-drain-hypervisor {hypervisor_name}'",
            timeout=SECONDS_IN_MINUTE * MINUTES_IN_HOUR * 2,
        )
        result = run_one_raw(command=command, node=self.control_node, is_safe=False)

        if not result:
            raise OpenstackMigrationError(
                f"Got no result when running {command} on {self.control_node_fqdn}, was expecting some output at "
                "least."
            )

    def quota_show(self) -> Dict[Union[str, OpenstackQuotaName], Any]:
        """Get the quotas for a project.

        Note that it will cast any known quota names to OpenstackQuotaName enums.
        """
        raw_quotas = self.run_formatted_as_dict("quota", "show")
        final_quotas: Dict[Union[str, OpenstackQuotaName], Any] = {}
        for quota_name, quota_value in raw_quotas.items():
            try:
                quota_entry = OpenstackQuotaEntry(name=OpenstackQuotaName(quota_name), value=quota_value)
                final_quotas[quota_entry.name] = quota_entry

            except ValueError:
                final_quotas[quota_name] = quota_value

        return final_quotas

    def quota_set(self, *quotas: OpenstackQuotaEntry) -> None:
        """Set a quota to the given value.

        Note that this sets the final value, not an increase.
        """
        quotas_cli = [quota.to_cli() for quota in quotas]

        self.run_raw("quota", "set", *quotas_cli)

    def quota_increase(self, *quota_increases: OpenstackQuotaEntry) -> None:
        """Set a quota to the given value.

        Note that this sets the final value, not an increase.
        """
        current_quotas = self.quota_show()

        increased_quotas: List[OpenstackQuotaEntry] = []

        for new_quota in quota_increases:
            if new_quota.name not in current_quotas:
                raise OpenstackError(f"Quota {new_quota} was not found in the remote Openstack API.")

            new_value = new_quota.value + current_quotas[new_quota.name].value
            increased_quotas.append(OpenstackQuotaEntry(name=new_quota.name, value=new_value))

        self.quota_set(*increased_quotas)
