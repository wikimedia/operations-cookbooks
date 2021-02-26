#!/usr/bin/env python3
# pylint: disable=unsubscriptable-object,too-many-arguments
"""Cloud Services Cookbooks"""
__title__ = __doc__
import base64
import json
import logging
from typing import Any, Callable, Dict, List

from spicerack.remote import RemoteHosts

LOGGER = logging.getLogger(__name__)
PHABRICATOR_BOT_CONFIG_FILE = "/etc/phabricator_ops-monitoring-bot.conf"


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

    return next(dst_node.run_sync(' '.join(full_command)))[1].message().decode()


def get_run_os(
    control_node: RemoteHosts,
    project: str,
) -> Callable[[str, ..., bool], Dict[str, Any]]:
    """Get a function to run an openstack command.

    The returned function, returns a structured result (loaded json).
    """

    def run_os(*command: List[str], is_safe: bool = False) -> Dict[str, Any]:
        # some commands don't have formatted output
        if 'delete' in command:
            format_args = []
        else:
            format_args = ["-f", "json"]

        full_command = [
            "env",
            f"OS_PROJECT_ID={project}",
            "wmcs-openstack",
            *command,
            *format_args,
        ]

        try:
            raw_result = next(control_node.run_sync(' '.join(full_command), is_safe=is_safe))[1].message().decode()
        except StopIteration:
            raw_result = "{}"

        return json.loads(raw_result)

    return run_os
