"""WMCS openstack - Unset a cloudvirt node maintenance

Usage example: wmcs.openstack.cloudvirt.unset_maintenance \
    --fqdn cloudvirt1013.eqiad.wmnet

"""
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.alerts import uptime_host
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.openstack.common import (
    AGGREGATES_FILE_PATH,
    Deployment,
    OpenstackAPI,
    OpenstackNotFound,
    get_control_nodes,
)

LOGGER = logging.getLogger(__name__)


class UnsetMaintenance(CookbookBase):
    """WMCS Openstack cookbook to unset a cloudvirt node maintenance."""

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
            help="FQDN of the cloudvirt to unset maintenance of.",
        )
        parser.add_argument(
            "--aggregates",
            required=False,
            default=None,
            help=(
                "Comma separated list of aggregate names to put the host in (by default will try to "
                f"use {AGGREGATES_FILE_PATH} if it exists, and fail otherwise). A safe choice would be just `ceph`"
            ),
        )
        parser.add_argument(
            "--downtime-id",
            required=False,
            default=None,
            help="Downtime id that you got when downtiming before, otherwise will remove all downtimes.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, UnsetMaintenanceRunner,)(
            fqdn=args.fqdn,
            aggregates=args.aggregates,
            downtime_id=args.downtime_id,
            spicerack=self.spicerack,
        )


class UnsetMaintenanceRunner(CookbookRunnerBase):
    """Runner for UnsetMaintenance."""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn: str,
        spicerack: Spicerack,
        downtime_id: Optional[str] = None,
        aggregates: Optional[str] = None,
    ):
        """Init."""
        self.fqdn = fqdn
        self.control_node_fqdn = get_control_nodes(deployment=Deployment.get_for_node(node=self.fqdn))[0]
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn=self.control_node_fqdn,
        )
        self.aggregates = aggregates
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.downtime_id = downtime_id

    def run(self) -> None:
        """Main entry point."""
        hostname = self.fqdn.split(".", 1)[0]
        try:
            self.openstack_api.aggregate_remove_host(aggregate_name="maintenance", host_name=hostname)
        except OpenstackNotFound as error:
            logging.info("%s", error)

        if self.aggregates:
            aggregates_to_add = [aggregate.strip() for aggregate in self.aggregates.split(",")]
        else:
            aggregates_to_add = [
                aggregate["name"]
                for aggregate in self.openstack_api.aggregate_load_from_host(
                    host=self.spicerack.remote().query(self.fqdn)
                )
            ]

        for aggregate_name in aggregates_to_add:
            try:
                self.openstack_api.aggregate_add_host(aggregate_name=aggregate_name, host_name=hostname)
            except OpenstackNotFound as error:
                logging.info("%s", error)

        uptime_host(spicerack=self.spicerack, host_name=hostname, silence_id=self.downtime_id)
        self.sallogger.log(message=f"Unset cloudvirt '{self.fqdn}' maintenance.")
        LOGGER.info(
            "Host %s now in out of maintenance mode. New VMs will be scheduled in it (aggregates: %s).",
            self.fqdn,
            ",".join(aggregates_to_add),
        )
