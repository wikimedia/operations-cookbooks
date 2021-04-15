#!/usr/bin/env python3
# pylint: disable=unsubscriptable-object,too-many-arguments
"""Cloud Services Cookbooks"""
__title__ = __doc__
import base64
import json
import logging
import re
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Union

from spicerack.remote import Remote, RemoteHosts

LOGGER = logging.getLogger(__name__)
PHABRICATOR_BOT_CONFIG_FILE = "/etc/phabricator_ops-monitoring-bot.conf"
DIGIT_RE = re.compile("([0-9]+)")


OpenstackID = str
OpenstackName = str


class NotFound(Exception):
    """Thrown when trying to get an element from Openstack gets no results."""


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

    def _run(self, *command: List[str], is_safe: bool = False):
        """Run an openstack command on a control node."""
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

        try:
            raw_result = (
                next(self._control_node.run_sync(" ".join(full_command), is_safe=is_safe))[1].message().decode()
            )
        except StopIteration:
            raw_result = "{}"

        return json.loads(raw_result)

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
