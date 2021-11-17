"""WMCS Toolforge - Remove an exsting etcd node from hiera

Usage examples:
    # Remove a node using the default node prefix
    cookbook wmcs.toolforge.remove_etcd_node_from_hiera \
        --project toolsbeta \
        --fqdn-to-remove toolsbeta-k8s-etcd-09.toolsbeta.eqiad1.wikimedia.cloud

    # Remove a node using a custom prefix (with the -test- after the project)
    cookbook wmcs.toolforge.remove_etcd_node_from_hiera \
        --project toolsbeta \
        --prefix toolsbeta-test-k8s-etcd \
        --fqdn-to-remove toolsbeta-test-k8s-etcd-09.toolsbeta.eqiad1.wikimedia.cloud

"""
import argparse
import json
import logging
from typing import Optional

import yaml
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OutputFormat, run_one

LOGGER = logging.getLogger(__name__)


class RemoveNodeFromHiera(CookbookBase):
    """WMCS Toolforge cookbook to remove a etcd node from hiera."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--prefix",
            required=False,
            default=None,
            help=("Prefix for etcd nodes in this project, will autogenerate by " "default (<project>-k8s-etcd)"),
        )
        parser.add_argument("--fqdn-to-remove", required=True, help="FQDN of the node to remove")

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get Runner"""
        return RemoveNodeFromHieraRunner(
            fqdn_to_remove=args.fqdn_to_remove,
            prefix=args.prefix,
            project=args.project,
            spicerack=self.spicerack,
        )


class RemoveNodeFromHieraRunner(CookbookRunnerBase):
    """Runner for RemoveNodeFromHiera"""

    def __init__(
        self,
        fqdn_to_remove: str,
        prefix: str,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.spicerack = spicerack
        self.project = project
        self.prefix = prefix
        self.fqdn_to_remove = fqdn_to_remove

    def run(self) -> Optional[int]:
        """Main entry point"""
        control_node = self.spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True)

        etcd_prefix = self.prefix if self.prefix is not None else f"{self.project}-k8s-etcd"
        response = run_one(
            node=control_node,
            command=["wmcs-enc-cli", "--openstack-project", self.project, "get_prefix_hiera", etcd_prefix],
            try_format=OutputFormat.YAML,
            is_safe=True,
        )
        # double yaml yep xd
        current_hiera_config = yaml.safe_load(response["hiera"])
        changed = False

        nodes = current_hiera_config.get("profile::toolforge::k8s::etcd_nodes", [])
        if self.fqdn_to_remove in nodes:
            nodes.pop(nodes.index(self.fqdn_to_remove))
            changed = True

        current_hiera_config["profile::toolforge::k8s::etcd_nodes"] = nodes

        alt_names = current_hiera_config.get("profile::base::puppet::dns_alt_names", [])
        if self.fqdn_to_remove in alt_names:
            alt_names.pop(alt_names.index(self.fqdn_to_remove))
            changed = True

        current_hiera_config["profile::base::puppet::dns_alt_names"] = alt_names

        if changed:
            # json is a one-line string, with only double quotes, nicer for
            # usage as cli parameter, and it's valid yaml :)
            current_hiera_config_str = json.dumps(current_hiera_config)
            LOGGER.info("New hiera config:\n%s", current_hiera_config_str)
            control_node.run_sync(
                f"wmcs-enc-cli --openstack-project {self.project} set_prefix_hiera {etcd_prefix} "
                f"'{current_hiera_config_str}'"
            )
        else:
            LOGGER.info("Hiera config was already correct.")

        return current_hiera_config
