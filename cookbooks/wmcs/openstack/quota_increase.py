"""WMCS openstack - increase a project's quota by a given amount

Usage example: wmcs.openstack.quota_increase \
    --project admin-monitoring \
    --gigabytes 30G \
    --instances 5

"""
import argparse
import logging
from typing import List, Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import (
    CommonOpts,
    OpenstackAPI,
    OpenstackQuotaEntry,
    OpenstackQuotaName,
    SALLogger,
    add_common_opts,
    with_common_opts,
)

LOGGER = logging.getLogger(__name__)


class QuotaIncrease(CookbookBase):
    """WMCS Openstack cookbook to increase the quota of a project."""

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
            "--gigabytes",
            required=False,
            help="Amount to increase the cinder space by (in G, ex. 10G or 10).",
        )
        parser.add_argument(
            "--ram",
            required=False,
            help="Amount to increase the ram by (in M or G, ex 10G, 250M, 250).",
        )
        parser.add_argument(
            "--cores",
            required=False,
            help="Amount to increase the cores/vcpus by.",
        )
        parser.add_argument(
            "--floating-ips",
            required=False,
            help="Amount to increase the floating ips by.",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(spicerack=self.spicerack, args=args, runner=QuotaIncreaseRunner)(
            project=args.project,
            vm_name=args.vm_name,
            cores=args.cores,
            floating_ips=args.floating_ips,
            ram=args.ram,
            gigabytes=args.gigabytes,
            control_node_fqdn=args.control_node_fqdn,
        )


class QuotaIncreaseRunner(CookbookRunnerBase):
    """Runner for QuotaIncrease"""

    def __init__(
        self,
        common_opts: CommonOpts,
        vm_name: str,
        cores: Optional[str],
        floating_ips: Optional[str],
        ram: Optional[str],
        gigabytes: Optional[str],
        control_node_fqdn: str,
        spicerack: Spicerack,
    ):  # pylint: disable=too-many-arguments
        """Init"""
        self.common_opts = common_opts
        self.vm_name = vm_name
        self.control_node_fqdn = control_node_fqdn
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn=control_node_fqdn,
            project=self.common_opts.project,
        )
        self.increases: List[OpenstackQuotaEntry] = []
        if cores:
            self.increases.append(
                OpenstackQuotaEntry.from_human_spec(
                    name=OpenstackQuotaName.CORES,
                    human_spec=cores,
                )
            )
        if gigabytes:
            self.increases.append(
                OpenstackQuotaEntry.from_human_spec(
                    name=OpenstackQuotaName.GIGABYTES,
                    human_spec=gigabytes,
                )
            )
        if floating_ips:
            self.increases.append(
                OpenstackQuotaEntry.from_human_spec(
                    name=OpenstackQuotaName.FLOATING_IPS,
                    human_spec=floating_ips,
                )
            )
        if ram:
            self.increases.append(
                OpenstackQuotaEntry.from_human_spec(
                    name=OpenstackQuotaName.RAM,
                    human_spec=ram,
                )
            )

        self.sallogger = SALLogger.from_common_opts(self.common_opts)

    def run(self) -> None:
        """Main entry point"""
        if not self.increases:
            return

        self.openstack_api.quota_increase(*self.increases)
        self.sallogger.log("Increased quotas by {self.increases}")
