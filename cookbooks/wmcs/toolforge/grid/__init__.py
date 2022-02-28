"""Manage grid servers for toolforge"""
__title__ = __doc__
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

import yaml
from cumin.transports import Command
from defusedxml import ElementTree
from spicerack.puppet import PuppetHosts
from spicerack.remote import Remote

from cookbooks.wmcs import run_one

LOGGER = logging.getLogger(__name__)


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

    @staticmethod
    def yaml_representer(dumper: yaml.Dumper, data: "GridQueueType") -> yaml.Node:
        """Serialize the structure as yaml."""
        return dumper.represent_scalar("!GridQueueType", data.name)


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

    @staticmethod
    def yaml_representer(dumper: yaml.Dumper, data: "GridQueueTypesSet") -> yaml.Node:
        """Serialize the structure as yaml."""
        return dumper.represent_sequence("!GridQueueTypesSet", data.types)


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

    @staticmethod
    def yaml_representer(dumper: yaml.Dumper, data: "GridQueueState") -> yaml.Node:
        """Serialize the structure as yaml."""
        return dumper.represent_scalar("!GridQueueState", data.name)


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

    @staticmethod
    def yaml_representer(dumper: yaml.Dumper, data: "GridQueueStatesSet") -> yaml.Node:
        """Serialize the structure as yaml."""
        return dumper.represent_sequence("!GridQueueStatesSet", data.states)


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
        raw_output = run_one(
            node=self._master_node,
            command=f"qhost -q -xml -h {host_fqdn}".split(),
            capture_errors=True,
            print_output=False,
            print_progress_bars=False,
        )
        for line in raw_output.split("\n"):
            if line.startswith("error: can't resolve hostname"):
                raise GridNodeNotFound(f"can't resolve hostname {host_fqdn}")

        parsed_xml = ElementTree.fromstring(raw_output)
        for node_xml in parsed_xml:
            if node_xml.attrib["name"] == "global":
                continue

            return GridNodeInfo.from_xml(xml_obj=node_xml)

        raise GridNodeNotFound(f"Unable to find node {host_fqdn}, output:\n{raw_output}")

    def depool_node(self, host_fqdn: str) -> None:
        """Depools a node from the grid.

        Raises:
            GridNodeNotFound: when the node is not found in the cluster

        """
        # call this just to report upstream an exception
        self.get_node_info(host_fqdn)
        hostname = host_fqdn.split(".")[0]
        self._master_node.run_sync(f"exec-manage depool {hostname}", print_output=False)

    def pool_node(self, hostname: str) -> None:
        """Repools a node from the grid.

        Raises:
            GridNodeNotFound: when the node is not found in the cluster

        """
        # call this just to report upstream an exception
        self.get_node_info(hostname)
        self._master_node.run_sync(f"exec-manage repool {hostname}", print_output=False, print_progress_bars=False)
