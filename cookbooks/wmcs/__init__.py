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
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Union

from cumin.transports import Command
from spicerack.remote import Remote, RemoteHosts

LOGGER = logging.getLogger(__name__)
PHABRICATOR_BOT_CONFIG_FILE = "/etc/phabricator_ops-monitoring-bot.conf"
DIGIT_RE = re.compile("([0-9]+)")
MINUTES_IN_HOUR = 60
SECONDS_IN_MINUTE = 60


OpenstackID = str
OpenstackName = str


class OpenstackError(Exception):
    """Parent class for all openstack related errors."""


class NotFound(OpenstackError):
    """Thrown when trying to get an element from Openstack gets no results."""


class MigrationError(OpenstackError):
    """Thrown when there's an issue with migration."""


class RuleDirection(Enum):
    """Directior for the security group roule."""

    ingress = auto()
    egress = auto()


class ServerGroupPolicy(Enum):
    """Affinity for the server group."""

    anti_affinity = "anti-affinity"
    affinity = "affinity"


class OpenstackAPI:
    """Class to interact with the Openstack API (undirectly for now)."""

    def __init__(self, remote: Remote, control_node_fqdn: str, project: OpenstackName = ""):
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
            flavor,
            "--image",
            image,
            "--network",
            network,
            "--hint",
            f"group={server_group_id}",
            "--wait",
            *security_group_options,
            name,
        )

    def security_group_list(self) -> List[Dict[str, Any]]:
        """Retrieve the list of security groups."""
        return self._run("security", "group", "list", is_safe=True)

    def security_group_create(self, name: OpenstackName, description: str) -> None:
        """Create a security group."""
        self._run("security", "group", "create", name, "--description", description)

    def security_group_rule_create(
        self, direction: RuleDirection, remote_group: OpenstackName, security_group: OpenstackName
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

        except NotFound:
            LOGGER.info("Creating security group %s...", security_group)
            self.security_group_create(
                name=security_group,
                description=description,
            )
            self.security_group_rule_create(
                direction=RuleDirection.egress, remote_group=security_group, security_group=security_group
            )
            self.security_group_rule_create(
                direction=RuleDirection.ingress, remote_group=security_group, security_group=security_group
            )

    def security_group_by_name(self, name: OpenstackName) -> Optional[Dict[str, Any]]:
        """Retrieve the security group info given a name.

        Raises NotFound if there's no security group found for the given name in the current project.
        """
        existing_security_groups = self.security_group_list()
        for security_group in existing_security_groups:
            if security_group["Project"] == self.project:
                if security_group["Name"] == name:
                    return security_group

        raise NotFound(f"Unable to find a security group with name {name}")

    def server_group_list(self) -> List[Dict[str, Any]]:
        """Get the list of server groups.

        Note:  it seems that on cli the project flag shows nothing :/ so we get the list all of them.
        """
        return self._run("server", "group", "list", is_safe=True)

    def server_group_create(self, name: OpenstackName, policy: ServerGroupPolicy) -> None:
        """Create a server group."""
        self._run(
            "server",
            "group",
            "create",
            "--policy",
            policy.value,
            name,
        )

    def server_group_ensure(self, server_group: OpenstackName) -> None:
        """Make sure that the given server group exists, create it if not there."""
        try:
            self.server_group_by_name(name=server_group)
            LOGGER.info("Server group %s already exists, not creating.", server_group)
        except NotFound:
            self.server_group_create(policy=ServerGroupPolicy.anti_affinity, name=server_group)

    def server_group_by_name(self, name: OpenstackName) -> Optional[Dict[str, Any]]:
        """Retrieve the server group info given a name.

        Raises NotFound if thereś no server group found with the given name.
        """
        all_server_groups = self.server_group_list()
        for server_group in all_server_groups:
            if server_group.get("Name", "") == name:
                return server_group

        raise NotFound(f"Unable to find a server group with name {name}")

    def aggregate_remove_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Remove the given host from the aggregate."""
        result = self._run("aggregate", "remove", "host", aggregate_name, host_name, capture_errors=True)
        if "HTTP 404" in result:
            raise NotFound(
                f"Node {host_name} was not found in aggregate {aggregate_name}, did you try using the hostname "
                "instead of the fqdn?"
            )

    def aggregate_add_host(self, aggregate_name: OpenstackName, host_name: OpenstackName) -> None:
        """Add the given host to the aggregate."""
        result = self._run("aggregate", "add", "host", aggregate_name, host_name, capture_errors=True)
        if "HTTP 404" in result:
            raise NotFound(
                f"Node {host_name} was not found in aggregate {aggregate_name}, did you try using the hostname "
                "instead of the fqdn?"
            )

    def drain_hypervisor(self, hypervisor_name: OpenstackName) -> None:
        """Drain a hypervisor."""
        command = Command(
            command=f"bash -c 'source /root/novaenv.sh && wmcs-drain-hypervisor {hypervisor_name}'",
            timeout=SECONDS_IN_MINUTE * MINUTES_IN_HOUR * 2,
        )
        try:
            next(self._control_node.run_sync(command, is_safe=False))

        except StopIteration:
            raise MigrationError(
                f"Got no result when running {command} on {self.control_node_fqdn}, was expecting some output at "
                "least."
            )


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


def dologmsg(
    message: str,
    project: str,
    task_id: Optional[str] = None,
    channel: str = "#wikimedia-cloud",
    host: str = "wm-bot.wm-bot.wmcloud.org",
    port: int = 64834,
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
            return
        # pylint: disable=broad-except
        except Exception as error:
            LOGGER.warning("Error trying to send a message to %s: %s", str(sockaddr), str(error))
        finally:
            my_socket.close()

    raise Exception(f"Unable to send log message to {host}:{port}, see previous logs for details")
