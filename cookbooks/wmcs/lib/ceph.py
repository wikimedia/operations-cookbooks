#!/usr/bin/env python3
"""Ceph related library functions and classes."""
import json
import logging
import time
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from spicerack import Remote, Spicerack
from wmflib.interactive import ask_confirmation

from cookbooks.wmcs import TestUtils, run_one_as_dict, run_one_formatted, run_one_raw
from cookbooks.wmcs.lib.alerts import SilenceID, downtime_alert, uptime_alert

LOGGER = logging.getLogger(__name__)
# List of alerts that are triggered by the cluster aside from the specifics for each node
CLUSTER_ALERT_MATCHES = [
    "alertname=Ceph Cluster Health",
    "alertname=Ceph OSDs Down",
    "alertname=Ceph Mon Quorum",
    "service=.*ceph.*",
]


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
    # explicit hard limit the pg log (don't use, deprecated feature)
    PGLOG_HARDLIMIT = "pglog_hardlimit"


@dataclass(frozen=True)
class CephClusterStatus:
    """Status of a CEPH cluster."""

    status_dict: Dict[str, Any]

    def get_osdmap_set_flags(self) -> Set[CephOSDFlag]:
        """Get osdmap set flags."""
        osd_maps = self.status_dict["health"]["checks"].get("OSDMAP_FLAGS")
        if not osd_maps:
            return set()

        raw_flags_line = osd_maps["summary"]["message"]
        if "flag(s) set" not in raw_flags_line:
            return set()

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

    def get_in_progress(self) -> Dict[str, Any]:
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
        structured_output = run_one_formatted(command=["lsblk", "--json"], node=self._node)
        if not isinstance(structured_output, dict):
            raise TypeError(f"Was expecting a dict, got {structured_output}")

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

    def __init__(self, remote: Remote, controlling_node_fqdn: str, spicerack: Spicerack):
        """Init."""
        self._remote = remote
        self._controlling_node_fqdn = controlling_node_fqdn
        self._controlling_node = self._remote.query(f"D{{{self._controlling_node_fqdn}}}", use_sudo=True)
        self._spicerack = spicerack

    def get_nodes(self) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        # There's usually a couple empty lines before the json data
        return run_one_as_dict(
            command=["ceph", "node", "ls", "-f", "json"], node=self._controlling_node, last_line_only=True
        )

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
        cluster_status_output = run_one_as_dict(command=["ceph", "status", "-f", "json"], node=self._controlling_node)
        return CephClusterStatus(status_dict=cluster_status_output)

    def set_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Set one of the osdmap flags."""
        set_osdmap_flag_result = run_one_raw(command=["ceph", "osd", "set", flag.value], node=self._controlling_node)
        if set_osdmap_flag_result != f"{flag.value} is set":
            raise CephFlagSetError(f"Unable to set `{flag.value}` on the cluster, got output: {set_osdmap_flag_result}")

    def unset_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Unset one of the osdmap flags."""
        unset_osdmap_flag_result = run_one_raw(
            command=["ceph", "osd", "unset", flag.value], node=self._controlling_node
        )
        if unset_osdmap_flag_result != f"{flag.value} is unset":
            raise CephFlagSetError(
                f"Unable to unset `{flag.value}` on the cluster, got output: {unset_osdmap_flag_result}"
            )

    def downtime_cluster_alerts(
        self, reason: str, duration: str = "4h", task_id: Optional[str] = None
    ) -> List[SilenceID]:
        """Downtime all the known cluster-wide alerts (the ones not related to a specific ceph node)."""
        silences = []
        # we match each set of alerts individually
        for alert_match in CLUSTER_ALERT_MATCHES:
            silences.append(
                downtime_alert(
                    spicerack=self._spicerack,
                    duration=duration,
                    task_id=task_id,
                    comment=f"Downtiming alert from cookbook - {reason}",
                    extra_queries=[alert_match],
                )
            )

        return silences

    def uptime_cluster_alerts(self, silences: Optional[List[SilenceID]]) -> None:
        """Enable again all the alert for the cluster.

        If specific silences are passed, only those are removed, if none are passed, it will remove any existing
        silence for cluster alerts.
        """
        if silences:
            for silence in silences:
                uptime_alert(spicerack=self._spicerack, silence_id=silence)

        else:
            # we match each individually
            for alert_match in CLUSTER_ALERT_MATCHES:
                uptime_alert(spicerack=self._spicerack, extra_queries=[alert_match])

    def set_maintenance(self, reason: str, force: bool = False, task_id: Optional[str] = None) -> List[SilenceID]:
        """Set maintenance and mute any cluster-wide alerts.

        Returns the list of alert silences, to pass back to unset_maintenance for example.
        """
        silences = self.downtime_cluster_alerts(task_id=task_id, reason=reason)
        cluster_status = self.get_cluster_status()
        if cluster_status.is_cluster_status_just_maintenance():
            LOGGER.info("Cluster already in maintenance status.")
            return silences

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
        return silences

    def unset_maintenance(self, force: bool = False, silences: Optional[List[SilenceID]] = None) -> None:
        """Unset maintenance and remove any cluster-wide alert silences.

        If no silences passed, it will remove all the existing silences for the cluster if any.
        """
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

        self.uptime_cluster_alerts(silences=silences)

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


# Poor man's namespace to compensate for the restriction to not create modules
@dataclass(frozen=True)
class CephTestUtils(TestUtils):
    """Utils to test ceph related code."""

    @staticmethod
    def get_status_dict(overrides: Dict[str, Any] = None) -> Dict[str, Any]:
        """Generate a stub status dict to use when creating CephStatus"""
        status_dict: Dict[str, Any] = {"health": {"status": {}, "checks": {}}}

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
        available_device: Dict[str, Any] = {"name": name, "type": device_type}
        if children is not None:
            available_device["children"] = children

        if mountpoint is not None:
            available_device["mountpoint"] = mountpoint

        return available_device
