#!/usr/bin/env python3
# pylint: disable=too-many-arguments
"""Openstack generic related code."""
import logging
import re
import time
from enum import Enum, auto
from typing import Any, Callable, Dict, List, NamedTuple, Optional, Type, Union, cast

import yaml
from cumin.transports import Command
from spicerack.remote import Remote, RemoteHosts

from cookbooks.wmcs.libs.common import (
    ArgparsableEnum,
    CommandRunnerMixin,
    OutputFormat,
    run_one_formatted,
    run_one_raw,
    simple_create_file,
)
from cookbooks.wmcs.libs.inventory import (
    OpenstackClusterName,
    OpenstackNodeRoleName,
    generic_get_node_cluster_name,
    get_node_inventory_info,
    get_nodes_by_role,
)

LOGGER = logging.getLogger(__name__)
AGGREGATES_FILE_PATH = "/etc/wmcs_host_aggregates.yaml"
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


OpenstackID = str
OpenstackName = str
OpenstackIdentifier = Union[OpenstackID, OpenstackName]


def get_control_nodes(cluster_name: OpenstackClusterName) -> List[str]:
    """Get all the FQDNs of the control nodes (in the future with netbox or similar)."""
    return get_nodes_by_role(cluster_name, role_name=OpenstackNodeRoleName.CONTROL)


def get_control_nodes_from_node(node: str) -> List[str]:
    """Get all the FQDNs of the control nodes from the cluster a given a node is part of."""
    return get_control_nodes(cluster_name=get_node_cluster_name(node))


def get_gateway_nodes(cluster_name: OpenstackClusterName) -> List[str]:
    """Get all the FQDNs of the gateway nodes (in the future with netbox or similar)."""
    return get_nodes_by_role(cluster_name, role_name=OpenstackNodeRoleName.GATEWAY)


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


class OpenstackError(Exception):
    """Parent class for all openstack related errors."""


class OpenstackNotFound(OpenstackError):
    """Thrown when trying to get an element from Openstack gets no results."""


class OpenstackMigrationError(OpenstackError):
    """Thrown when there's an issue with migration."""


class OpenstackBadQuota(OpenstackError):
    """Thrown when the quota given is not known or incorrect."""


class OpenstackRuleDirection(ArgparsableEnum):
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

    def __str__(self):
        """Convert a OpenstackQuotaEntry to a formatted string for display."""
        return f"{self.value} {self.name.value}"

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


class OpenstackServerGroupPolicy(ArgparsableEnum):
    """Affinity for the server group."""

    SOFT_ANTI_AFFINITY = "soft-anti-affinity"
    ANTI_AFFINITY = "anti-affinity"
    AFFINITY = "affinity"
    SOFT_AFFINITY = "soft-affinity"


class OpenstackAPI(CommandRunnerMixin):
    """Class to interact with the Openstack API (indirectly for now)."""

    def __init__(
        self,
        remote: Remote,
        cluster_name: OpenstackClusterName = OpenstackClusterName.EQIAD1,
        project: OpenstackName = "",
    ):
        """Init."""
        self.project = project
        self.cluster_name = cluster_name
        self.control_node_fqdn = get_control_nodes(cluster_name)[0]
        self.control_node = remote.query(f"D{{{self.control_node_fqdn}}}", use_sudo=True)
        super().__init__(command_runner_node=self.control_node)

    def _get_full_command(self, *command: str, json_output: bool = True, project_as_arg: bool = False):
        # some commands don't have formatted output
        if json_output:
            format_args = ["-f", "json"]
        else:
            format_args = []
        if "delete" in command:
            format_args = []

        # some commands require passing the project as an argument and cannot use OS_PROJECT_ID
        if project_as_arg:
            return ["wmcs-openstack", *command, self.project, *format_args]

        return ["env", f"OS_PROJECT_ID={self.project}", "wmcs-openstack", *command, *format_args]

    def host_list(self, **kwargs) -> List[str]:
        """Returns a list of openstack hosts (i.e, hypervisors)."""
        host_list = self.run_formatted_as_list("host", "list", "--sort-descending", is_safe=True, **kwargs)
        return [h["Host Name"] for h in host_list if re.match(r"cloudvirt\d", h["Host Name"])]

    def get_nodes_domain(self) -> str:
        """Return the domain of the cluster handled by this controller.

        Note: the cloudcontrols usually use the wikimedia.org domain, not taken into account here.
        """
        info = get_node_inventory_info(node=self.control_node_fqdn)
        return f"{info.site_name.value}.wmnet"

    def create_service_ip(self, ip_name: OpenstackName, network: OpenstackIdentifier, **kwargs) -> Dict[str, Any]:
        """Create a service IP with a specified name

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_formatted_as_dict("port", "create", "--network", _quote(network), _quote(ip_name), **kwargs)

    def attach_service_ip(self, ip_address: str, server_port_id: OpenstackIdentifier, **kwargs) -> str:
        """Attach a specified service ip address to the specified port

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_raw(
            "port",
            "set",
            "--allowed-address",
            f"ip-address={ip_address}",
            _quote(server_port_id),
            json_output=False,
            **kwargs,
        )

    def detach_service_ip(self, ip_address: str, mac_addr: str, server_port_id: OpenstackIdentifier, **kwargs) -> str:
        """Detach a specified service ip address from the specified port

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        return self.run_raw(
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

    def recordset_create(self, zone_id, record_type, name, record, **kwargs) -> Dict[str, Any]:
        """Get zone record for specified dns zone"""
        return self.run_formatted_as_dict(
            "recordset", "create", "--type", record_type, "--record", record, zone_id, name, **kwargs
        )

    def server_show(self, vm_name: OpenstackIdentifier) -> Dict[str, Any]:
        """Get the information for a VM."""
        return self.run_formatted_as_dict("server", "show", vm_name, is_safe=True)

    def server_list(self, long: bool = False, **kwargs) -> List[Dict[str, Any]]:
        """Retrieve the list of servers for the project.

        Any extra kwargs will be passed to the RemoteHosts.run_sync function.
        """
        _long = "--long" if long else ""
        return self.run_formatted_as_list("server", "list", _long, is_safe=True, **kwargs)

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

    def server_force_reboot(self, name_to_reboot: OpenstackName) -> None:
        """Force reboot a VM.

        Note that the name_to_reboot is the name of the VM as registered in
        Openstack, that's probably not the FQDN (and hopefully the hostname,
        but maybe not).
        """
        self.run_raw("server", "reboot", "--hard", name_to_reboot, json_output=False, is_safe=False)

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
        server_group_id: Optional[OpenstackID] = None,
        security_group_ids: Optional[List[OpenstackID]] = None,
        properties: Optional[Dict[str, str]] = None,
        availability_zone: Optional[str] = None,
    ) -> OpenstackIdentifier:
        """Create a server and return the ID of the created server.

        Note: You will probably want to add the server to the 'default' security group at least.
        """
        security_group_options = []
        if security_group_ids:
            for security_group_id in security_group_ids:
                security_group_options.extend(["--security-group", security_group_id])

        server_group_options = []
        if server_group_id:
            server_group_options.extend(["--hint", f"group={server_group_id}"])

        properties_opt = []
        if properties:
            for i in properties:
                properties_opt.extend(["--property", f"{i}='{properties[i]}'"])

        availability_zone_opt = []
        if availability_zone:
            availability_zone_opt.extend(["--availability-zone", availability_zone])

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
            *properties_opt,
            *availability_zone_opt,
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
        # OS_PROJECT_ID=PROJECT wmcs-openstack quota show displays the admin project!
        # This must be run as wmcs-openstack quota show PROJECT
        raw_quotas = self.run_formatted_as_dict("quota", "show", project_as_arg=True)
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

        self.run_raw("quota", "set", *quotas_cli, json_output=False, project_as_arg=True)

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

        # Validate quota was updated as expected
        new_quotas = self.quota_show()
        for new_quota in increased_quotas:
            if new_quota.value != new_quotas[new_quota.name].value:
                raise OpenstackError(
                    f"{new_quotas[new_quota.name]} quota of {new_quotas[new_quota.name].value} "
                    f"does not match expected value of {new_quota.value}"
                )

    def flavor_create(
        self,
        vcpus: int,
        ram_gb: int,
        disk_gb: int,
        public: bool,
        project: str,
        disk_read_iops_sec: int,
        disk_write_iops_sec: int,
        disk_total_bytes_sec: int,
        generation: int = 3,
    ) -> Dict[str, Any]:
        """Create a new flavor."""
        name = f"g{generation}.cores{vcpus}.ram{ram_gb}.disk{disk_gb}"

        if not public:
            # per-project flavors still have to be uniquely named
            name += f".{project}"

        command = [
            "flavor",
            "create",
            f"--ram={ram_gb * 1024}",
            f"--disk={disk_gb}",
            f"--vcpus={vcpus}",
            '--property "aggregate_instance_extra_specs:ceph=true"',
            f'--property "quota:disk_read_iops_sec={disk_read_iops_sec}"',
            f'--property "quota:disk_write_iops_sec={disk_write_iops_sec}"',
            f'--property "quota:disk_total_bytes_sec={disk_total_bytes_sec}"',
        ]
        if public:
            command.append("--public")
        else:
            command.extend(
                [
                    "--private",
                    f"--project='{project}'",
                ]
            )
        command.append(name)

        return self.run_formatted_as_dict(*command)

    def role_list_assignments(self, user_name: OpenstackName) -> List[Dict[str, Any]]:
        """List the assignments for a user in the project."""
        return self.run_formatted_as_list(
            "role", "assignment", "list", f"--project={self.project}", f"--user={user_name}"
        )

    def role_add(self, role_name: OpenstackName, user_name: OpenstackName) -> None:
        """Add a user to a role for a project, it will not fail if the user is already has that role."""
        self.run_raw("role", "add", f"--project={self.project}", f"--user={user_name}", role_name, json_output=False)

    def role_remove(self, role: OpenstackIdentifier, user_name: OpenstackName) -> None:
        """Remove a user from a role for a project, it will not fail if the user is not in that that role."""
        self.run_raw("role", "remove", f"--project={self.project}", f"--user={user_name}", role, json_output=False)


def get_node_cluster_name(node: str) -> OpenstackClusterName:
    """Wrapper casting to the specific openstack type."""
    return cast(OpenstackClusterName, generic_get_node_cluster_name(node))
