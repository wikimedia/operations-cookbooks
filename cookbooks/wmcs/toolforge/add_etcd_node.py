"""WMCS Toolforge - Add a new etcd node to a toolforge installation.

Usage example:
    cookbook wmcs.toolforge.add_etcd_node \
        --project toolsbeta \
        --etcd-prefix toolsbeta-k8s-test-etcd

"""
# pylint: disable=too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.toolforge.etcd.add_node_to_cluster import AddNodeToCluster
from cookbooks.wmcs.vps.create_instance_with_prefix import CreateInstanceWithPrefix

LOGGER = logging.getLogger(__name__)


class ToolforgeAddEtcdNode(CookbookBase):
    """WMCS Toolforge cookbook to add a new etcd node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--project",
            required=True,
            help="Openstack project where the toolforge installation resides.",
        )
        parser.add_argument(
            "--etcd-prefix",
            required=False,
            default=None,
            help="Prefix for the k8s etcd nodes, default is <project>-k8s-etcd.",
        )
        parser.add_argument(
            "--skip-puppet-bootstrap",
            action="store_true",
            help=(
                "Skip all the puppet bootstraping section, useful if you already did it and you are rerunning, or if "
                "you did it manually."
            ),
        )
        parser.add_argument(
            "--flavor",
            required=False,
            default=None,
            help=(
                "Flavor for the new instance (will use the same as the latest existing one by default, ex. "
                "g2.cores4.ram8.disk80, ex. 06c3e0a1-f684-4a0c-8f00-551b59a518c8)."
            ),
        )
        parser.add_argument(
            "--image",
            required=False,
            default=None,
            help=(
                "Image for the new instance (will use the same as the latest existing one by default, ex. "
                "debian-10.0-buster, ex. 64351116-a53e-4a62-8866-5f0058d89c2b)"
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeAddEtcdNodeRunner(
            etcd_prefix=args.etcd_prefix,
            skip_puppet_bootstrap=args.skip_puppet_bootstrap,
            project=args.project,
            image=args.image,
            flavor=args.flavor,
            spicerack=self.spicerack,
        )


class ToolforgeAddEtcdNodeRunner(CookbookRunnerBase):
    """Runner for ToolforgeAddEtcdNode"""

    def __init__(
        self,
        etcd_prefix: str,
        skip_puppet_bootstrap: bool,
        project: str,
        spicerack: Spicerack,
        image: Optional[str] = None,
        flavor: Optional[str] = None,
    ):
        """Init"""
        self.etcd_prefix = etcd_prefix
        self.skip_puppet_bootstrap = skip_puppet_bootstrap
        self.project = project
        self.spicerack = spicerack
        self.image = image
        self.flavor = flavor

    def run(self) -> None:
        """Main entry point"""
        etcd_prefix = self.etcd_prefix if self.etcd_prefix is not None else f"{self.project}-k8s-etcd"

        start_args = [
            "--project",
            self.project,
            "--prefix",
            etcd_prefix,
            "--security-group",
            f"{self.project}-k8s-full-connectivity",
            "--server-group",
            self.etcd_prefix,
        ]
        if self.image:
            start_args.extend(["--image", self.image])

        if self.flavor:
            start_args.extend(["--flavor", self.flavor])

        create_instance_cookbook = CreateInstanceWithPrefix(spicerack=self.spicerack)
        new_member = create_instance_cookbook.get_runner(
            args=create_instance_cookbook.argument_parser().parse_args(start_args)
        ).create_instance()

        add_node_to_cluster_args = [
            "--project",
            self.project,
            "--etcd-prefix",
            etcd_prefix,
            "--new-member-fqdn",
            new_member.server_fqdn,
        ]
        if self.skip_puppet_bootstrap:
            add_node_to_cluster_args.append("--skip-puppet-bootstrap")
        add_node_to_cluster_cookbook = AddNodeToCluster(spicerack=self.spicerack)
        add_node_to_cluster_cookbook.get_runner(
            args=add_node_to_cluster_cookbook.argument_parser().parse_args(add_node_to_cluster_args),
        ).run()

        LOGGER.info("Added a new node %s to etcd cluster", new_member.server_fqdn)
