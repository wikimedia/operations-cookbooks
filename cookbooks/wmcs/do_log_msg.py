"""WMCS openstack - Log a SAL message

Usage example: wmcs.do_log_msg \
    --msg "I just changed some config in cloudvirt1020"
    --task-id T424242

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import dologmsg

LOGGER = logging.getLogger(__name__)


class Dologmsg(CookbookBase):
    """WMCS cookbook to log a SAL message."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--project",
            required=False,
            default="admin",
            help="Project on SAL to log the message for.",
        )
        parser.add_argument(
            "--msg",
            required=True,
            help="Message to log.",
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to the message (ex. T123456)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return DologmsgRunner(
            msg=args.msg,
            project=args.project,
            task_id=args.task_id,
            spicerack=self.spicerack,
        )


class DologmsgRunner(CookbookRunnerBase):
    """Runner for Dologmsg."""

    def __init__(
        self,
        msg: str,
        project: str,
        spicerack: Spicerack,
        task_id: Optional[str] = None,
    ):
        """Init."""
        self.msg = msg
        self.project = project
        self.task_id = task_id
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point."""
        dologmsg(project=self.project, message=self.msg, task_id=self.task_id)
