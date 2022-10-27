r"""WMCS openstack - add a new flavor

Usage example: wmcs.openstack.add_flavor \
    --cluster-name eqiad1 \
    --project wikidumpparse \
    --ram-gb 2 \
    --vcpus 8 \
    --disk-gb 2
"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class AddFlavor(CookbookBase):
    """WMCS Openstack cookbook to create a new flavor."""

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
            "--cluster-name",
            required=True,
            choices=list(OpenstackClusterName),
            type=OpenstackClusterName,
            help="Openstack cluster/deployment to act on.",
        )
        parser.add_argument(
            "--ram-gb",
            required=True,
            type=int,
            help="Size in GB for the RAM of this new flavor.",
        )
        parser.add_argument(
            "--vcpus",
            required=True,
            type=int,
            help="Number of virtual CPUs.",
        )
        parser.add_argument(
            "--disk-gb",
            required=True,
            type=int,
            help="Size in GB for the OS disk.",
        )
        parser.add_argument(
            "--public",
            required=False,
            default=False,
            action="store_true",
            help=(
                "If passed, will make this flavor public (see "
                "https://wikitech.wikimedia.org/wiki/Portal:Cloud_VPS/Admin/VM_flavors#General_flavor_guidelines for "
                "details)"
            ),
        )
        parser.add_argument(
            "--disk-read-iops-sec",
            required=False,
            type=int,
            default=5000,
            help="Rate limiting quota for read iops (the default should be good for most cases)",
        )
        parser.add_argument(
            "--disk-total-bytes-sec",
            required=False,
            type=int,
            default=200_000_000,
            help="Rate limiting quota for total bytes/sec (the default should be good for most cases)",
        )
        parser.add_argument(
            "--disk-write-iops-sec",
            required=False,
            type=int,
            default=500,
            help="Rate limiting quota for write iops (the default should be good for most cases)",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(spicerack=self.spicerack, args=args, runner=AddFlavorRunner)(
            vcpus=args.vcpus,
            ram_gb=args.ram_gb,
            disk_gb=args.disk_gb,
            public=args.public,
            disk_read_iops_sec=args.disk_read_iops_sec,
            disk_write_iops_sec=args.disk_write_iops_sec,
            disk_total_bytes_sec=args.disk_total_bytes_sec,
            spicerack=self.spicerack,
            cluster_name=args.cluster_name,
        )


class AddFlavorRunner(CookbookRunnerBase):
    """Runner for AddFlavor"""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        common_opts: CommonOpts,
        vcpus: int,
        ram_gb: int,
        disk_gb: int,
        public: bool,
        disk_read_iops_sec: int,
        disk_write_iops_sec: int,
        disk_total_bytes_sec: int,
        spicerack: Spicerack,
        cluster_name: OpenstackClusterName,
    ):
        """Init"""
        self.common_opts = common_opts
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(), cluster_name=cluster_name, project=self.common_opts.project
        )
        self.vcpus = vcpus
        self.ram_gb = ram_gb
        self.disk_gb = disk_gb
        self.public = public
        self.disk_read_iops_sec = disk_read_iops_sec
        self.disk_write_iops_sec = disk_write_iops_sec
        self.disk_total_bytes_sec = disk_total_bytes_sec
        self.sallogger = SALLogger.from_common_opts(self.common_opts)

    def run(self) -> None:
        """Main entry point"""
        new_flavor = self.openstack_api.flavor_create(
            project=self.common_opts.project,
            vcpus=self.vcpus,
            ram_gb=self.ram_gb,
            disk_gb=self.disk_gb,
            public=self.public,
            disk_read_iops_sec=self.disk_read_iops_sec,
            disk_write_iops_sec=self.disk_write_iops_sec,
            disk_total_bytes_sec=self.disk_total_bytes_sec,
        )
        self.sallogger.log(f"Created new flavor: {new_flavor['name']} (id:{new_flavor['id']})")
