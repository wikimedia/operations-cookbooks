r"""WMCS openstack - set a cloudvirt node in maintenance

Usage example: wmcs.openstack.cloudvirt.set_maintenance \
    --fqdn cloudvirt1013.eqiad.wmnet

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.alerts import downtime_host
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI, OpenstackNotFound, get_node_cluster_name

LOGGER = logging.getLogger(__name__)


class SetMaintenance(CookbookBase):
    """WMCS Openstack cookbook to set a cloudvirt node in maintenance."""

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
            help="FQDN of the cloudvirt to set in maintenance.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, SetMaintenanceRunner,)(
            fqdn=args.fqdn,
            spicerack=self.spicerack,
        )


class SetMaintenanceRunner(CookbookRunnerBase):
    """Runner for SetMaintenance."""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn: str,
        spicerack: Spicerack,
    ):
        """Init."""
        self.fqdn = fqdn
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            cluster_name=get_node_cluster_name(node=self.fqdn),
        )
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point."""
        hostname = self.fqdn.split(".", 1)[0]
        downtime_id = downtime_host(spicerack=self.spicerack, host_name=hostname, comment="Setting maintenance mode.")
        self.openstack_api.aggregate_persist_on_host(host=self.spicerack.remote().query(self.fqdn))

        try:
            self.openstack_api.aggregate_remove_host(aggregate_name="ceph", host_name=hostname)
        except OpenstackNotFound as error:
            logging.info("%s", error)

        try:
            self.openstack_api.aggregate_add_host(aggregate_name="maintenance", host_name=hostname)
        except OpenstackNotFound as error:
            logging.info("%s", error)

        self.sallogger.log(
            message=f"Set cloudvirt '{self.fqdn}' maintenance (downtime id: {downtime_id}, use this to unset)."
        )
        LOGGER.info("Host %s now in maintenance mode. No new VMs will be scheduled in it.", self.fqdn)
