"""WMCS openstack network tests - Run a network testsuite

Usage example:
  cookbook wmcs.openstack.network.tests --deployment codfw1dev
  cookbook wmcs.openstack.network.tests --deployment eqiad1

"""
import argparse
import logging
from typing import Optional, Tuple, List
from enum import Enum

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

LOGGER = logging.getLogger(__name__)


class Deployment(Enum):
    """Deployment enumerate"""

    EQIAD1 = "eqiad1"
    CODFW1DEV = "codfw1dev"

    def __str__(self):
        """String representation"""
        return self.value


all_control_nodes = {
    Deployment.EQIAD1: [
        "cloudcontrol1003.wikimedia.org",
        "cloudcontrol1004.wikimedia.org",
        "cloudcontrol1005.wikimedia.org",
    ],
    Deployment.CODFW1DEV: [
        "cloudcontrol2001-dev.wikimedia.org",
        "cloudcontrol2003-dev.wikimedia.org",
        "cloudcontrol2004-dev.wikimedia.org",
    ],
}


class NetworkTests(CookbookBase):
    """WMCS openstack cookbook to run automated network tests/checks."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )

        parser.add_argument(
            "-d",
            "--deployment",
            help="openstack deployment where to run the tests",
            type=Deployment,
            choices=list(Deployment),
            default=Deployment.CODFW1DEV,
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return NetworkTestRunner(
            deployment=args.deployment,
            spicerack=self.spicerack,
        )


class NetworkTestParsingError(Exception):
    """Custom exception class for NetworkTest parsing errors."""


class NetworkTestRunner(CookbookRunnerBase):
    """Runner for NetworkTestsTODO"""

    def __init__(self, deployment: str, spicerack: Spicerack):
        """Init"""
        self.deployment = deployment
        self.spicerack = spicerack

    def _parse_output(self, output_lines: List[str]) -> Tuple[int, int, int]:
        """Parse run_sync() results"""
        passed = failed = total = -1

        for line in output_lines:
            if line.startswith("[cmd-checklist-runner] INFO: --- passed tests: "):
                passed = int(line.split(" ")[-1])
                continue

            if line.startswith("[cmd-checklist-runner] INFO: --- failed tests: "):
                failed = int(line.split(" ")[-1])
                continue

            if line.startswith("[cmd-checklist-runner] INFO: --- total tests: "):
                total = int(line.split(" ")[-1])
                continue

        if passed < 0 or failed < 0 or total < 0:
            raise NetworkTestParsingError(
                "Unable to parse the output of the checklist runner"
            )

        return passed, failed, total

    def run(self) -> Optional[int]:
        """Main entry point"""
        # TODO: once we can run cumin with the puppetdb backend from our laptop
        # this ugly harcoding can be replaced to something like:
        # query = f"P{{O:wmcs::openstack::{self.deployment}::control}}"
        control_nodes = ",".join(all_control_nodes[self.deployment])
        query = f"D{{{control_nodes}}}"
        remote_hosts = self.spicerack.remote().query(query, use_sudo=True)

        # only interested in one control node
        for i in remote_hosts.split(len(remote_hosts)):
            control_node = i
            break

        results = control_node.run_sync(
            "cmd-checklist-runner --config /etc/networktests/networktests.yaml",
            print_progress_bars=False,
            is_safe=True,
        )

        for _, output in results:
            output_lines = output.message().decode().splitlines()
            passed, failed, total = self._parse_output(output_lines)
            break  # should have been executed in just 1 node anyway

        if total < 1:
            LOGGER.warning(f"{self.__class__.__name__}: no tests were run!")

        if failed > 0:
            LOGGER.error(f"{self.__class__.__name__}: {failed} failed tests detected!")
            return 1

        LOGGER.info(f"{self.__class__.__name__}: {passed}/{total} passed tests.")
        return 0
