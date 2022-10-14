#!/usr/bin/env python3
"""Ceph related library functions and classes."""
import json
import logging
import re
import time
from copy import copy, deepcopy
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set, Union, cast

from spicerack import Remote, Spicerack
from spicerack.remote import RemoteExecutionError
from wmflib.interactive import ask_confirmation

from cookbooks.wmcs.libs.alerts import SilenceID, downtime_alert, uptime_alert
from cookbooks.wmcs.libs.common import ArgparsableEnum, CommandRunnerMixin, TestUtils, run_one_formatted, run_one_raw
from cookbooks.wmcs.libs.inventory import (
    CephClusterName,
    CephNodeRoleName,
    generic_get_node_cluster_name,
    get_node_inventory_info,
    get_nodes_by_role,
    get_osd_drives_count,
)

LOGGER = logging.getLogger(__name__)
# List of alerts that are triggered by the cluster aside from the specifics for each node
OSD_EXPECTED_OS_DRIVES = 2


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


class CephOSDFlag(ArgparsableEnum):
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


class OSDClass(ArgparsableEnum):
    """Supported OSD classes."""

    HDD = "hdd"
    SSD = "ssd"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, status_str: str) -> "OSDClass":
        """Get the osd class object from a string like the one from `ceph osd tree -f json`."""
        try:
            return cls(status_str)
        except ValueError:
            return cls.UNKNOWN


class OSDStatus(ArgparsableEnum):
    """Known ceph osd statuses."""

    UP = "up"
    DOWN = "down"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, status_str: str) -> "OSDStatus":
        """Get the status object from a string like the one from `ceph osd tree -f json`."""
        try:
            return cls(status_str)
        except ValueError:
            return cls.UNKNOWN


@dataclass(frozen=True)
class OSDTreeEntry:
    """Class to bundle OSD data together."""

    osd_id: int
    name: str
    device_class: OSDClass
    status: OSDStatus
    crush_weight: float

    @classmethod
    def from_json_data(cls, json_data: Dict[str, Any]) -> "OSDTreeEntry":
        """Get an osd class from the osd entry in the output of `ceph osd tree -f json`."""
        return cls(
            osd_id=json_data["id"],
            name=json_data["name"],
            device_class=OSDClass.from_str(json_data["device_class"]),
            status=OSDStatus.from_str(json_data["status"]),
            crush_weight=json_data["crush_weight"],
        )


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


class CephOSDNodeController:
    """Controller for a CEPH OSD node."""

    def __init__(self, remote: Remote, node_fqdn: str):
        """Init."""
        self._remote = remote
        self.node_fqdn = node_fqdn
        self._node = self._remote.query(f"D{{{self.node_fqdn}}}", use_sudo=True)

    @classmethod
    def _is_device_available(cls, device_info: Dict[str, Any]) -> bool:
        def _is_disk() -> bool:
            return device_info.get("type") == "disk"

        def _does_not_have_partitions() -> bool:
            return not device_info.get("children")

        def _its_not_mounted() -> bool:
            return not device_info.get("mountpoint")

        return _is_disk() and _does_not_have_partitions() and _its_not_mounted()

    def do_lsblk(self) -> List[Dict[str, Any]]:
        """Simple lsblk on the host to get the devices."""
        structured_output = run_one_formatted(
            command=["lsblk", "--json"],
            node=self._node,
            print_output=False,
            print_progress_bars=False,
        )
        if not isinstance(structured_output, dict):
            raise TypeError(f"Was expecting a dict, got {structured_output}")

        if "blockdevices" not in structured_output:
            raise CephMalformedInfo(
                f"Missing 'blockdevices' on lsblk output: {json.dumps(structured_output, indent=4)}"
            )

        return structured_output["blockdevices"]

    def get_available_devices(self) -> List[str]:
        """Get the current available devices in the node."""
        return [
            f"/dev/{device_info['name']}"
            for device_info in self.do_lsblk()
            if self._is_device_available(device_info=device_info)
        ]

    def zap_device(self, device_path: str) -> None:
        """Zap the given device.

        NOTE: this destroys all the information in the device!
        """
        run_one_raw(command=["ceph-volume", "lvm", "zap", device_path], node=self._node)

    def initialize_and_start_osd(self, device_path: str) -> None:
        """Setup and start a new osd on the given device."""
        run_one_raw(command=["ceph-volume", "lvm", "create", "--bluestore", "--data", device_path], node=self._node)

    def add_all_available_devices(self, interactive: bool = True) -> None:
        """Discover and add all the available devices of the node as new OSDs."""
        for device_path in self.get_available_devices():
            if interactive:
                ask_confirmation(f"I'm going to destroy and create a new OSD on {self.node_fqdn}:{device_path}.")

            self.zap_device(device_path=device_path)
            self.initialize_and_start_osd(device_path=device_path)

    def check_jumbo_frames_to(self, dst_ip: str) -> bool:
        """Check if this node is ready to be setup as a new osd."""
        try:
            run_one_raw(
                command=[
                    "ping",
                    # the following is to avoid fragmenting packages
                    "-M",
                    "do",
                    # force ipv4
                    "-4",
                    # count, we use two because sometimes after reboot the
                    # first ping to the new network is lost by the router
                    # (while resolving arp addresses)
                    "-c",
                    "2",
                    # timeout
                    "-W",
                    "1",
                    # the following size generates a 9000 jumbo frame packet
                    "-s",
                    "8972",
                    dst_ip,
                ],
                node=self._node,
                print_output=False,
                print_progress_bars=False,
            )
        except RemoteExecutionError as err:
            LOGGER.warning("Failed to ping %s with a jumbo frame: %s", dst_ip, str(err))
            return False

        return True


class CephClusterController(CommandRunnerMixin):
    """Controller for a CEPH cluster."""

    CLUSTER_ALERT_MATCH = "service=~.*ceph.*"

    def __init__(self, remote: Remote, cluster_name: CephClusterName, spicerack: Spicerack):
        """Init."""
        self._remote = remote
        self.controlling_node_fqdn = get_mon_nodes(cluster_name)[0]
        self._controlling_node = self._remote.query(f"D{{{self.controlling_node_fqdn}}}", use_sudo=True)
        self.expected_osd_drives_per_host = get_osd_drives_count(cluster_name)
        self._spicerack = spicerack
        super().__init__(command_runner_node=self._controlling_node)

    def _get_full_command(self, *command: str, json_output: bool = True, project_as_arg: bool = False):
        if json_output:
            format_args = ["-f", "json"]
        else:
            format_args = []

        return ["ceph", *command, *format_args]

    def get_nodes(self) -> Dict[str, Any]:
        """Get the nodes currently in the cluster."""
        # There's usually a couple empty lines before the json data
        return self.run_formatted_as_dict("node", "ls", last_line_only=True)

    def get_nodes_domain(self) -> str:
        """Get the network domain for the nodes in the cluster."""
        info = get_node_inventory_info(node=self.controlling_node_fqdn)
        return f"{info.site_name.value}.wmnet"

    def change_controlling_node(self) -> None:
        """Change the current node being used to interact with the cluster for another one."""
        current_monitor_name = self.controlling_node_fqdn.split(".", 1)[0]
        nodes = self.get_nodes()
        try:
            another_monitor = next(node_host for node_host in nodes["mon"].keys() if node_host != current_monitor_name)
        except StopIteration as error:
            raise CephNoControllerNode(
                f"Unable to find any other mon node to control the cluster, got nodes: {nodes}"
            ) from error

        self.controlling_node_fqdn = f"{another_monitor}.{self.get_nodes_domain()}"
        self._controlling_node = self._remote.query(f"D{{{self.controlling_node_fqdn}}}", use_sudo=True)
        LOGGER.info("Changed to node %s to control the CEPH cluster.", self.controlling_node_fqdn)

    def get_cluster_status(self) -> CephClusterStatus:
        """Get the current cluster status."""
        cluster_status_output = self.run_formatted_as_dict("status")
        return CephClusterStatus(status_dict=cluster_status_output)

    def set_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Set one of the osdmap flags."""
        set_osdmap_flag_result = self.run_raw("osd", "set", flag.value, json_output=False)
        if not re.match(f"(^|\n){flag.value} is set", set_osdmap_flag_result):
            raise CephFlagSetError(f"Unable to set `{flag.value}` on the cluster, got output: {set_osdmap_flag_result}")

    def unset_osdmap_flag(self, flag: CephOSDFlag) -> None:
        """Unset one of the osdmap flags."""
        unset_osdmap_flag_result = self.run_raw("osd", "unset", flag.value, json_output=False)
        if not re.match(f"(^|\n){flag.value} is unset", unset_osdmap_flag_result, re.MULTILINE):
            raise CephFlagSetError(
                f"Unable to unset `{flag.value}` on the cluster, got output: {unset_osdmap_flag_result}"
            )

    def set_osd_class(self, osd_id: int, osd_class: OSDClass) -> None:
        """Change an osd class (ex. from hdd to ssd).

        Note that `osd_id` is the number of the osd, for example, for osd.195, that would be the integer 195.
        """
        self.run_raw("osd", "crush", "rm-device-class", f"{osd_id}", json_output=False)
        self.run_raw("osd", "crush", "set-device-class", osd_class.value, f"{osd_id}", json_output=False)

    def downtime_cluster_alerts(
        self, reason: str, duration: str = "4h", task_id: Optional[str] = None
    ) -> List[SilenceID]:
        """Downtime all the known cluster-wide alerts (the ones not related to a specific ceph node)."""
        silences = []
        # There's only one alert left
        silences.append(
            downtime_alert(
                spicerack=self._spicerack,
                duration=duration,
                task_id=task_id,
                comment=f"Downtiming alert from cookbook - {reason}",
                extra_queries=[self.CLUSTER_ALERT_MATCH],
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
            uptime_alert(spicerack=self._spicerack, extra_queries=[self.CLUSTER_ALERT_MATCH])

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
                    "%ds have passed, but the cluster is still not healthy, waiting another %d (timeout=%d)...",
                    cur_time - start_time,
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

    def get_osd_tree(self) -> Dict[str, Any]:
        """Retrieve the osd tree, already parsed into a tree structure."""

        def _get_expanded_node(
            plain_node: Dict[str, Any], all_nodes: Dict[int, Dict[str, Any]]
        ) -> Union[Dict[str, Any], OSDTreeEntry]:

            # We expect the "osd" nodes to always be leaf nodes of the tree
            if plain_node.get("type") == "osd":
                return OSDTreeEntry.from_json_data(plain_node)

            # We expect other node types to always have a "children" attribute (can be an empty list)
            if plain_node.get("children", None) is None:
                raise Exception(f"Unexpected leaf node that is not an OSD: {plain_node}")

            children_ids = plain_node["children"]
            children = [_get_expanded_node(all_nodes[child_id], all_nodes) for child_id in children_ids]
            expanded_node = copy(plain_node)
            expanded_node["children"] = children
            return expanded_node

        def _get_nested_nodes_tree(nodes_list: List[Dict[str, Any]]) -> Union[Dict[str, Any], OSDTreeEntry]:
            id_to_nodes: Dict[int, Dict[str, Any]] = {node["id"]: node for node in nodes_list}
            root_node = next(node for node in nodes_list if node["type"] == "root")
            return _get_expanded_node(plain_node=root_node, all_nodes=id_to_nodes)

        flat_nodes = self.run_formatted_as_dict("osd", "tree")
        return {
            "nodes": _get_nested_nodes_tree(nodes_list=flat_nodes["nodes"]),
            # TODO: update the following to a useful structure if it's ever needed
            "stray": flat_nodes["stray"],
        }

    def get_all_osd_ips(self) -> Set[str]:
        """Returns all the known ips for all the osd, deduplicated.

        This includes the public and cluster ips, useful to run tests.
        """
        osd_dump = self.run_formatted_as_dict(
            "osd",
            "dump",
            print_output=False,
            print_progress_bars=False,
        )
        all_osd_ips: Set[str] = set()
        for osd in osd_dump.get("osds", []):
            public_addr = osd["public_addr"].split(":", 1)[0]
            all_osd_ips.add(public_addr)
            cluster_addr = osd["cluster_addr"].split(":", 1)[0]
            all_osd_ips.add(cluster_addr)

        return all_osd_ips

    def check_if_osd_ready_for_bootstrap(self, osd_controller: CephOSDNodeController) -> List[str]:
        """Check if a node is ready to be added as osd to the cluster.

        Returns a list of any failures that happened.
        """
        failures: List[str] = []

        LOGGER.info("Checking that jumbo frames are allowed to all other nodes in the cluster...")
        for other_node_ip in self.get_all_osd_ips():
            if not osd_controller.check_jumbo_frames_to(other_node_ip):
                failures.append(f"Unable to send jumbo frames to {other_node_ip} from node {osd_controller.node_fqdn}")

        LOGGER.info("Checking that we have the right amount of drives in the host...")
        host_devices = osd_controller.do_lsblk()
        total_expected_devices = OSD_EXPECTED_OS_DRIVES + self.expected_osd_drives_per_host
        if len(host_devices) != total_expected_devices:
            failures.append(
                f"The host has {len(host_devices)}, when we are expecting {total_expected_devices} "
                f"({self.expected_osd_drives_per_host} for osds, and {OSD_EXPECTED_OS_DRIVES} for the os)"
            )

        LOGGER.info("Checking that we have enough free drives in the host...")
        available_devices = osd_controller.get_available_devices()
        if len(available_devices) > self.expected_osd_drives_per_host:
            failures.append(
                f"We expected to have at least {OSD_EXPECTED_OS_DRIVES} drives reserved for OS, but it seems we "
                f"would use some of them ({available_devices}), maybe the raid is not properly setup?"
            )

        LOGGER.info("Checking that we have enough OS dedicated drives in the host...")
        # example of soft-raid device:
        # {"name":"sda", "maj:min":"8:0", "rm":false, "size":"447.1G", "ro":false, "type":"disk", "mountpoint":null,
        #    "children": [
        #       {"name":"sda1", ...},
        #       {"name":"sda2", ...
        #          "children": [
        #             {"name":"md0", ...
        #                "children": [
        #                   {"name":"vg0-swap", ...},
        #                   {"name":"vg0-root", ...},
        #                   {"name":"vg0-srv", ...}
        #                ]
        #             }
        #          ]
        #       }
        #    ]
        # },
        devices_with_soft_raid_on_them = [
            device
            for device in host_devices
            if device.get("children", [])
            and any(
                child.get("children", []) and child["children"] and child["children"][0].get("name", "") == "md0"
                for child in device["children"]
            )
        ]
        if len(devices_with_soft_raid_on_them) != OSD_EXPECTED_OS_DRIVES:
            failures.append(
                "It seems we don't have the expected raids setup on the OS devices, I was expecting "
                f"{OSD_EXPECTED_OS_DRIVES} setup in software raid, but got {devices_with_soft_raid_on_them}"
            )

        return failures

    def is_osd_host_valid(self, osd_tree: Dict[str, Any], hostname: str) -> bool:
        """Validates a specific hostname in a given OSD tree.

        It checks that the hostname is present in the tree, and it has the expected attributes.
        """
        host_node = [n for n in osd_tree["nodes"]["children"] if n["name"] == hostname]

        if len(host_node) != 1:
            LOGGER.warning("Expected 1 node in the OSD tree with name='%s' but found %d", hostname, len(host_node))
            return False

        if len(host_node[0]["children"]) != self.expected_osd_drives_per_host:
            LOGGER.warning(
                "Expected %d OSDs in the OSD tree for host '%s' but found %d",
                self.expected_osd_drives_per_host,
                hostname,
                len(host_node[0]["children"]),
            )
            return False

        return True


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
        name: str = "sddummy_non_matching_part",
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


def get_mon_nodes(cluster_name: CephClusterName) -> List[str]:
    """Get the list of mon nodes given a cluster."""
    return get_nodes_by_role(cluster_name, role_name=CephNodeRoleName.MON)


def get_osd_nodes(cluster_name: CephClusterName) -> List[str]:
    """Get the list of osd nodes given a cluster."""
    return get_nodes_by_role(cluster_name, role_name=CephNodeRoleName.OSD)


def get_node_cluster_name(node: str) -> CephClusterName:
    """Wrapper casting to the right type."""
    return cast(CephClusterName, generic_get_node_cluster_name(node))
