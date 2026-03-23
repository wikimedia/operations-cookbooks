"""Check network topology details of Kubernetes nodes"""

import logging
from argparse import ArgumentParser, Namespace
from collections import defaultdict

from cumin import NodeSet
from prettytable import PrettyTable
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, CookbookInitSuccess

from cookbooks.sre.k8s import (
    host_expected_bgp_session_count,
    host_has_l2_adjacency_to_lvs,
)

logger = logging.getLogger(__name__)


class PrintNetworkTopology(CookbookBase):
    """Print network topology details of Kubernetes nodes:

    - Expected BGP session count (e.g. pairing with core routers or Tor switches)
    - L2 adjacency to LVS server
    """

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments for all the sre.discovery cookbooks."""
        parser = super().argument_parser()
        parser.add_argument(
            "-e",
            "--expand",
            action="store_true",
            help="Expand the output to one line per host (instead of folded NodeSet).",
        )

        parser.add_argument(
            "hosts", help="Hosts to be checked (specified in Cumin query syntax)."
        )
        return parser

    def get_runner(self, args: Namespace) -> "PrintNetworkTopologyRunner":
        """As specified by Spicerack API."""
        return PrintNetworkTopologyRunner(args, self.spicerack)


class PrintNetworkTopologyRunner(CookbookRunnerBase):
    """Check and print network topology details."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Check and print network topology details."""
        self.args = args
        self.spicerack = spicerack
        self.hosts_cumin_query = args.hosts
        # Resolve the Cumin query to a NodeSet
        self.hosts = spicerack.remote().query(self.hosts_cumin_query).hosts

        table = PrettyTable(
            ["Hosts", "VLAN", "Expected BGP session count", "L2 adjacency to LVS"]
        )
        results = defaultdict(list)
        for host in self.hosts:
            netbox_server = self.spicerack.netbox_server(
                host.split(".")[0], read_write=False
            )
            vlan = netbox_server.access_vlan
            bgp_session_count = host_expected_bgp_session_count(netbox_server)
            l2_adjacency = host_has_l2_adjacency_to_lvs(netbox_server)
            properties = (vlan, bgp_session_count, l2_adjacency)
            if self.args.expand:
                table.add_row([host, *properties])
            else:
                results[properties].append(host)

        if not self.args.expand:
            # Now fold the hosts back into NodeSets based on the common summary
            for properties, hosts in results.items():
                host_set = NodeSet.fromlist(hosts)
                table.add_row([host_set, *properties])

        print(table)
        raise CookbookInitSuccess()

    def run(self):
        """Run is never executed since we exit early via CookbookInitSuccess."""
