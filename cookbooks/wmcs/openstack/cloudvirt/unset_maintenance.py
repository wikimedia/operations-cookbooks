"""WMCS openstack - Unset a cloudvirt node maintenance

Usage example: wmcs.openstack.cloudvirt.unset_maintenance \
    --control-node-fqdn cloudcontrol1003.wikimedia.org
    --fqdn cloudvirt1013.eqiad.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.icinga import ICINGA_DOMAIN, IcingaHosts

from cookbooks.wmcs import (
    AGGREGATES_FILE_PATH,
    CommonOpts,
    OpenstackAPI,
    OpenstackNotFound,
    add_common_opts,
    dologmsg,
    with_common_opts,
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
            "--control-node-fqdn",
            required=False,
            default="cloudcontrol1003.wikimedia.org",
            help="FQDN of the control node to orchestrate from.",
        )
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

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, UnsetMaintenanceRunner,)(
            fqdn=args.fqdn,
            control_node_fqdn=args.control_node_fqdn,
            aggregates=args.aggregates,
            spicerack=self.spicerack,
        )


class UnsetMaintenanceRunner(CookbookRunnerBase):
    """Runner for UnsetMaintenance."""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn: str,
        control_node_fqdn: str,
        spicerack: Spicerack,
        aggregates: Optional[str] = None,
    ):
        """Init."""
        self.common_opts = common_opts
        self.fqdn = fqdn
        self.control_node_fqdn = control_node_fqdn
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn=control_node_fqdn,
        )
        self.aggregates = aggregates
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point."""
        dologmsg(common_opts=self.common_opts, message=f"Unsetting cloudvirt '{self.fqdn}' maintenance.")
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

        icinga_hosts = IcingaHosts(
            icinga_host=self.spicerack.remote().query(self.spicerack.dns().resolve_cname(ICINGA_DOMAIN), use_sudo=True),
            target_hosts=[self.fqdn],
        )
        icinga_hosts.remove_downtime()
        dologmsg(common_opts=self.common_opts, message=f"Unset cloudvirt '{self.fqdn}' maintenance.")
        LOGGER.info(
            "Host %s now in out of maintenance mode. New VMs will be scheduled in it (aggregates: %s).",
            self.fqdn,
            ",".join(aggregates_to_add),
        )
