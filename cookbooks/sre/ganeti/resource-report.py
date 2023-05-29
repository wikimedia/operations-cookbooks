"""Add a new node to a Ganeti cluster"""
import logging

from dataclasses import dataclass
from typing import Union

import urllib3

from prettytable import PrettyTable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

logger = logging.getLogger(__name__)

# Suppress:
# SubjectAltNameWarning: Certificate for ganeti01.svc.eqiad.wmnet has no `subjectAltName`
# falling back to check for a `commonName` for now.
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)


@dataclass
class GanetiGroupInfo:
    """Class to hold ganeti group info."""

    name: str
    mfree: float = 0
    dfree: float = 0
    nodes: int = 0
    instances: int = 0

    def table_row(self) -> list:
        """Return a row suitable for PrettyTable.add_row()"""
        return [
            self.name,
            self.nodes,
            self.instances,
            self._sizeof_fmt(self.mfree),
            self._sizeof_fmt(self.mfree_avg),
            self._sizeof_fmt(self.dfree),
            self._sizeof_fmt(self.dfree_avg),
        ]

    @staticmethod
    def _sizeof_fmt(num: Union[int, float], suffix: str = "B") -> str:
        """Convert a int in MB to a human readable format."""
        for unit in ["Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
            if abs(num) < 1024.0:
                return f"{num:3.1f}{unit}{suffix}"
            num /= 1024.0

        return f"{num:.1f}Yi{suffix}"

    @property
    def mfree_avg(self) -> float:
        """Return the mean free Memory."""
        return self.mfree / self.nodes

    @property
    def dfree_avg(self) -> float:
        """Return the mean free disk space."""
        return self.dfree / self.nodes


class GanetiGroupReport(CookbookBase):
    """Produce a report on resource usage broken down by group.

    Usage example:
        cookbook -d sre.ganeti.resource-report eqiad
    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "cluster",
            help=(
                "The Ganeti cluster short name, as reported in Netbox as a Cluster Group: "
                "https://netbox.wikimedia.org/virtualization/cluster-groups/"
            ),
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiGroupReportRunner(args, self.spicerack)


class GanetiGroupReportRunner(CookbookRunnerBase):
    """Produce a report on resource usage broken down by group Runner."""

    def __init__(self, args, spicerack):
        """Add a new node to a Ganeti cluster."""
        ganeti = spicerack.ganeti()
        self.rapi = ganeti.rapi(args.cluster)
        self.table = PrettyTable()
        self.table.field_names = [
            "Group",
            "Nodes",
            "Instances",
            "MFree",
            "MFree avg",
            "DFree",
            "DFree avg",
        ]

    def run(self):
        """Get the ganeti report data."""
        groups_info = {}
        for group in self.rapi.groups(bulk=True):
            name = group["uuid"]
            if name not in groups_info:
                groups_info[name] = GanetiGroupInfo(group["name"])
            groups_info[name].nodes = group['node_cnt']
        for node in self.rapi.nodes(bulk=True):
            name = node["group.uuid"]
            groups_info[name].dfree += node["dfree"]
            groups_info[name].mfree += node["mfree"]
            groups_info[name].instances += node["pinst_cnt"]
        for group in groups_info.values():
            self.table.add_row(group.table_row())
        print(self.table)
