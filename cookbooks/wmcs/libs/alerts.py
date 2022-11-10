#!/usr/bin/env python3
"""Alert and downtime related library functions and classes."""
import getpass
import logging
import socket
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from spicerack import Spicerack
from spicerack.remote import Remote, RemoteHosts

from cookbooks.wmcs.libs.common import run_one_formatted_as_list, run_one_raw, wrap_with_sudo_icinga

SilenceID = str

ALERTMANAGER_HOST = "alert1001.wikimedia.org"
LOGGER = logging.getLogger(__name__)


@dataclass
class AlertManager:
    """Class to handle alert manager silences."""

    node: RemoteHosts

    @classmethod
    def from_remote(cls, remote: Remote) -> "AlertManager":
        """Get an AlertManager instance from a remote."""
        node = remote.query(f"D{{{ALERTMANAGER_HOST}}}")
        return cls(node=node)

    def get_silences(self, query: str) -> List[Dict[str, Any]]:
        """Get all silences enabled filtering with query.

        Some examples of 'query':
        * "alertname=foo"
        * "instance=bar"
        * "'alertname=~.*foo.*' 'service=~.*ceph.*'"
        """
        return run_one_formatted_as_list(node=self.node, command=["amtool", "--output=json", "silence", "query", query])

    def downtime_alert(
        self, alert_name: str, comment: str, duration: str = "1h", extra_queries: Optional[List[str]] = None
    ) -> SilenceID:
        """Add a silence for an alert.

        extra_queries is a list of label/match pairs, for example:
        * ["service=~.*ceph.*", "instance=cloudcontrtol1005"]

        Examples of 'alert_name':
        * "Ceph Cluster Health"

        Examples of 'duration':
        * 1h -> one hour
        * 2d -> two days
        """
        if alert_name:
            query = f"'alertname={alert_name}'"
        else:
            query = ""

        if extra_queries:
            for new_query in extra_queries:
                query = f"{query} '{new_query}'"

        command = [
            "amtool",
            "--output=json",
            "silence",
            "add",
            f'--duration="{duration}"',
            f"--comment='{comment}'",
            query,
        ]
        return run_one_raw(node=self.node, command=command)

    def uptime_alert(self, alert_name: Optional[str] = None, extra_queries: Optional[List[str]] = None) -> None:
        """Remove a silence for an alert.

        extra_queries is a list of label/match pairs, for example:
        * ["service=~.*ceph.*", "instance=cloudcontrtol1005"]

        Examples of 'alert_name':
        * "Ceph Cluster Health"
        """
        if alert_name:
            query = f"'alertname={alert_name}'"
        else:
            query = ""

        if extra_queries:
            for new_query in extra_queries:
                query = f"{query} '{new_query}'"

        if not query:
            raise ValueError("Either alert_name or extra_queries should be passed.")

        existing_silences = self.get_silences(query=query)
        to_expire = [silence["id"] for silence in existing_silences]

        if not to_expire:
            LOGGER.info("No silences matching '%s' found.", query)
            return

        command = [
            "amtool",
            "--output=json",
            "silence",
            "expire",
        ] + to_expire
        run_one_raw(node=self.node, command=command)

    def downtime_host(self, host_name: str, comment: str, duration: Optional[str] = None) -> SilenceID:
        """Add a silence for a host.

        Examples of 'host_name':
        * cloudcontrol1005
        * cloudcephmon1001

        Examples of 'duration':
        * 1h -> one hour
        * 2d -> two days
        """
        command = [
            "amtool",
            "--output=json",
            "silence",
            "add",
            f'--duration="{duration or "1h"}"',
            f"--comment='{comment}'",
            f"instance=~'{host_name}(:[0-9]+)?'",
        ]
        return run_one_raw(node=self.node, command=command)

    def expire_silence(self, silence_id: str) -> None:
        """Expire a silence."""
        command = [
            "amtool",
            "--output=json",
            "silence",
            "expire",
            silence_id,
        ]
        run_one_raw(node=self.node, command=command)

    def uptime_host(self, host_name: str) -> None:
        """Expire all silences for a host."""
        existing_silences = self.get_silences(query=f"instance={host_name}")
        to_expire = [silence["id"] for silence in existing_silences]

        if not to_expire:
            LOGGER.info("No silences for 'instance=%s' found.", host_name)
            return

        command = [
            "amtool",
            "--output=json",
            "silence",
            "expire",
        ] + to_expire
        run_one_raw(node=self.node, command=command)


def downtime_host(
    spicerack: Spicerack,
    host_name: str,
    duration: Optional[str] = None,
    comment: Optional[str] = None,
    task_id: Optional[str] = None,
) -> SilenceID:
    """Do whatever it takes to downtime a host.

    Examples of 'host_name':
    * cloudcontrol1005
    * cloudcephmon1001

    Examples of 'duration':
    * 1h -> one hour
    * 2d -> two days
    """
    postfix = f"- from cookbook ran by {getpass.getuser()}@{socket.gethostname()}"
    if task_id:
        postfix = f" ({task_id}) {postfix}"
    if comment:
        final_comment = comment + postfix
    else:
        final_comment = "No comment" + postfix

    alert_manager = AlertManager.from_remote(spicerack.remote())
    silence_id = alert_manager.downtime_host(host_name=host_name, duration=duration, comment=final_comment)

    icinga_hosts = wrap_with_sudo_icinga(my_spicerack=spicerack).icinga_hosts(target_hosts=[host_name])
    icinga_hosts.downtime(reason=spicerack.admin_reason(reason=comment or "No comment", task_id=task_id))

    return silence_id


def uptime_host(spicerack: Spicerack, host_name: str, silence_id: Optional[SilenceID] = None) -> None:
    """Do whatever it takes to uptime a host, if silence_id passed, only that silence will be expired.

    Examples of 'host_name':
    * cloudcontrol1005
    * cloudcephmon1001
    """
    alert_manager = AlertManager.from_remote(spicerack.remote())
    if silence_id:
        alert_manager.expire_silence(silence_id=silence_id)
    else:
        alert_manager.uptime_host(host_name=host_name)

    icinga_hosts = wrap_with_sudo_icinga(my_spicerack=spicerack).icinga_hosts(target_hosts=[host_name])
    icinga_hosts.remove_downtime()


def downtime_alert(
    spicerack: Spicerack,
    alert_name: str = "",
    duration: str = "1h",
    comment: Optional[str] = None,
    task_id: Optional[str] = None,
    extra_queries: Optional[List[str]] = None,
) -> SilenceID:
    """Do whatever it takes to downtime a host.

    Examples of 'alert_name':
    * "Ceph Cluster Health"

    Examples of 'duration':
    * 1h -> one hour
    * 2d -> two days
    """
    postfix = f"- from cookbook ran by {getpass.getuser()}@{socket.gethostname()}"
    if task_id:
        postfix = f" ({task_id}) {postfix}"
    if comment:
        final_comment = comment + postfix
    else:
        final_comment = "No comment" + postfix

    alert_manager = AlertManager.from_remote(spicerack.remote())
    return alert_manager.downtime_alert(
        alert_name=alert_name, duration=duration, comment=final_comment, extra_queries=extra_queries
    )


def uptime_alert(
    spicerack: Spicerack,
    alert_name: Optional[str] = None,
    silence_id: Optional[SilenceID] = None,
    extra_queries: Optional[List[str]] = None,
) -> None:
    """Do whatever it takes to uptime an alert, if silence_id passed, only that silence will be expired.

    Examples of 'alert_name':
    * "Ceph Cluster Health"
    """
    alert_manager = AlertManager.from_remote(spicerack.remote())
    if silence_id:
        alert_manager.expire_silence(silence_id=silence_id)
    elif alert_name or extra_queries:
        alert_manager.uptime_alert(alert_name=alert_name, extra_queries=extra_queries)
    else:
        raise ValueError("You must pass either silence_id or alert_name and/or extra_queries")
