"""WMCS Toolforge - Remove an instance from a project.

Usage example:
    cookbook wmcs.vps.remove_instance \
        --project toolsbeta \
        --server-name toolsbeta-k8s-test-etcd-08

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import get_run_os

LOGGER = logging.getLogger(__name__)


class RemoveInstance(CookbookBase):
    """WMCS VPS cookbook to stop an instance."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage (ex. toolsbeta).")
        parser.add_argument(
            "--server-name",
            required=True,
            help="Name of the server to remove (without domain, ex. toolsbeta-test-k8s-etcd-9)."
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return RemoveInstanceRunner(
            project=args.project,
            name_to_remove=args.server_name,
            spicerack=self.spicerack,
        )


class RemoveInstanceRunner(CookbookRunnerBase):
    """Runner for RemoveInstance."""

    def __init__(
        self,
        project: str,
        name_to_remove: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.run_os = get_run_os(
            control_node=spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True),
            project=project,
        )

        self.project = project
        self.name_to_remove = name_to_remove
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        all_project_server_infos = self.run_os("server", "list", is_safe=True)
        if not any(
            info for info in all_project_server_infos
            if info["Name"] == self.name_to_remove
        ):
            LOGGER.warning(
                "Unable to find server %s in project %s. Please review the project and server name.",
                self.name_to_remove,
                self.project,
            )
            return

        self.run_os("server", "delete", self.name_to_remove, is_safe=False)
