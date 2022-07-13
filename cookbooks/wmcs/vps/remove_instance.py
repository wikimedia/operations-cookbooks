"""WMCS Toolforge - Remove an instance from a project.

Usage example:
    cookbook wmcs.vps.remove_instance \
        --project toolsbeta \
        --server-name toolsbeta-k8s-test-etcd-08

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

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
        add_common_opts(parser)
        parser.add_argument(
            "--server-name",
            required=True,
            help="Name of the server to remove (without domain, ex. toolsbeta-test-k8s-etcd-9).",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RemoveInstanceRunner,)(
            name_to_remove=args.server_name,
            spicerack=self.spicerack,
        )


class RemoveInstanceRunner(CookbookRunnerBase):
    """Runner for RemoveInstance."""

    def __init__(
        self,
        common_opts: CommonOpts,
        name_to_remove: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn="cloudcontrol1003.wikimedia.org",
            project=self.common_opts.project,
        )

        self.name_to_remove = name_to_remove
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        if not self.openstack_api.server_exists(self.name_to_remove, print_output=False):
            LOGGER.warning(
                "Unable to find server %s in project %s. Please review the project and server name.",
                self.name_to_remove,
                self.common_opts.project,
            )
            return

        self.sallogger.log(message=f"removing instance {self.name_to_remove}")
        self.openstack_api.server_delete(name_to_remove=self.name_to_remove)
