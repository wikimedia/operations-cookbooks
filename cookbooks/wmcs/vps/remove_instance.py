"""WMCS Toolforge - Remove an instance from a project.

Usage example:
    cookbook wmcs.vps.remove_instance \
        --project toolsbeta \
        --server-name toolsbeta-k8s-test-etcd-08

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackAPI, dologmsg

LOGGER = logging.getLogger(__name__)


class RemoveInstance(CookbookBase):
    """WMCS VPS cookbook to stop an instance."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage (ex. toolsbeta).")
        parser.add_argument(
            "--server-name",
            required=True,
            help="Name of the server to remove (without domain, ex. toolsbeta-test-k8s-etcd-9).",
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this operation (ex. T123456)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return RemoveInstanceRunner(
            project=args.project,
            name_to_remove=args.server_name,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class RemoveInstanceRunner(CookbookRunnerBase):
    """Runner for RemoveInstance."""

    def __init__(
        self,
        project: str,
        name_to_remove: str,
        spicerack: Spicerack,
        task_id: Optional[str] = None,
    ):
        """Init"""
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=project
        )

        self.project = project
        self.name_to_remove = name_to_remove
        self.spicerack = spicerack
        self.task_id = task_id

    def run(self) -> Optional[int]:
        """Main entry point"""
        all_project_server_infos = self.openstack_api.server_list(print_output=False)
        if not any(info for info in all_project_server_infos if info["Name"] == self.name_to_remove):
            LOGGER.warning(
                "Unable to find server %s in project %s. Please review the project and server name.",
                self.name_to_remove,
                self.project,
            )
            return

        dologmsg(
            project=self.project,
            message=f"removing instance {self.name_to_remove}",
            task_id=self.task_id,
        )
        self.openstack_api.server_delete(name_to_remove=self.name_to_remove)
