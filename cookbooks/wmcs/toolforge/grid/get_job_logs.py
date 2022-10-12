r"""WMCS Toolforge - grid - get job logs

Gets the grid logs for the given job (not the job output itself)

Usage example:
    cookbook wmcs.toolforge.grid.get_job_logs \
        --project toolsbeta \
        --job-id 12345
"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.grid import GridController

LOGGER = logging.getLogger(__name__)


class ToolforgeGridGetJobLogs(CookbookBase):
    """Toolforge cookbook to get the logs for a job"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument("--job-id", required=True, type=int, help="Id of the job to get the logs for.")
        parser.add_argument(
            "--master-node-fqdn",
            required=False,
            default=None,
            help=(
                "Name of the grid master node, will use <project>-sgegrid-master.<project>.eqiad1.wikimedia.cloud by "
                "default."
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeGridGetJobLogsRunner(
            master_node_fqdn=args.master_node_fqdn
            or f"{args.project}-sgegrid-master.{args.project}.eqiad1.wikimedia.cloud",
            project=args.project,
            job_id=args.job_id,
            spicerack=self.spicerack,
        )


class ToolforgeGridGetJobLogsRunner(CookbookRunnerBase):
    """Runner for ToolforgeGridGetJobLogs"""

    def __init__(
        self,
        master_node_fqdn: str,
        project: str,
        job_id: int,
        spicerack: Spicerack,
    ):
        """Init"""
        self.master_node_fqdn = master_node_fqdn
        self.project = project
        self.spicerack = spicerack
        self.job_id = job_id
        self.grid_controller = GridController(remote=self.spicerack.remote(), master_node_fqdn=self.master_node_fqdn)

    def run(self) -> None:
        """Main entry point"""
        print(f"###### Jobs logs for job {self.job_id}")
        print(self.grid_controller.get_job_error_logs(job_id=self.job_id))
