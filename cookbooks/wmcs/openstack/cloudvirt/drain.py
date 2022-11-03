r"""WMCS openstack - Drain a cloudvirt node

Usage example: wmcs.openstack.cloudvirt.drain \
    --fqdn cloudvirt1013.eqiad.wmnet

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, WMCSCookbookRunnerBase, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI, get_node_cluster_name
from cookbooks.wmcs.openstack.cloudvirt.set_maintenance import SetMaintenance

LOGGER = logging.getLogger(__name__)


class Drain(CookbookBase):
    """WMCS Openstack cookbook to drain a cloudvirt node."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
        parser.add_argument(
            "--fqdn",
            required=True,
            help="FQDN of the cloudvirt to drain.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, DrainRunner,)(
            fqdn=args.fqdn,
            spicerack=self.spicerack,
        )


class DrainRunner(WMCSCookbookRunnerBase):
    """Runner for Drain"""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.fqdn = fqdn
        super().__init__(spicerack=spicerack)
        self.openstack_api = OpenstackAPI(remote=spicerack.remote(), cluster_name=get_node_cluster_name(node=self.fqdn))
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run_with_proxy(self) -> None:
        """Main entry point"""
        self.sallogger.log(message=f"Draining {self.fqdn}")
        set_maintenance_cookbook = SetMaintenance(spicerack=self.spicerack)
        set_maintenance_cookbook.get_runner(
            args=set_maintenance_cookbook.argument_parser().parse_args(
                args=[
                    "--fqdn",
                    self.fqdn,
                ]
                + self.common_opts.to_cli_args(),
            )
        ).run()
        hypervisor_name = self.fqdn.split(".", 1)[0]
        self.openstack_api.drain_hypervisor(hypervisor_name=hypervisor_name)
        self.sallogger.log(message=f"Drained {self.fqdn}")
