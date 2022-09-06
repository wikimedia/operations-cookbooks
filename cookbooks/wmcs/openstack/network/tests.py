r"""WMCS openstack network tests - Run a network testsuite

Usage example:
  cookbook wmcs.openstack.network.tests --cluster_name codfw1dev
  cookbook wmcs.openstack.network.tests --cluster_name eqiad1

Documentation:
  https://wikitech.wikimedia.org/wiki/Portal:Cloud_VPS/Admin/Network/Tests

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import CmdChecklist
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import get_control_nodes

LOGGER = logging.getLogger(__name__)


class NetworkTests(CookbookBase):
    """WMCS openstack cookbook to run automated network tests/checks."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )

        parser.add_argument(
            "--cluster-name",
            help="openstack cluster_name where to run the tests",
            type=OpenstackClusterName,
            choices=list(OpenstackClusterName),
            default=OpenstackClusterName.CODFW1DEV,
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return NetworkTestRunner(
            cluster_name=args.cluster_name,
            spicerack=self.spicerack,
        )


class NetworkTestRunner(CookbookRunnerBase):
    """Runner for NetworkTests"""

    def __init__(self, cluster_name: OpenstackClusterName, spicerack: Spicerack):
        """Init"""
        self.cluster_name: OpenstackClusterName = cluster_name
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        control_node = get_control_nodes(self.cluster_name)[0]
        query = f"D{{{control_node}}}"
        remote_host = self.spicerack.remote().query(query, use_sudo=True)

        checklist = CmdChecklist(
            name="Cloud VPS network tests", remote_hosts=remote_host, config_file="/etc/networktests/networktests.yaml"
        )
        results = checklist.run(print_progress_bars=False)
        return checklist.evaluate(results)
