"""WMCS Toolforge - Add a new etcd node to hiera

Usage examples:
    # Add a node using the default node prefix
    cookbook wmcs.toolforge.etcd.add_node_to_hiera \
        --project toolsbeta \
        --fqdn-to-add toolsbeta-k8s-etcd-09.toolsbeta.eqiad1.wikimedia.cloud

    # Add a node using a custom prefix (ex. with the -test- after the project)
    cookbook wmcs.toolforge.etcd.add_node_to_hiera \
        --project toolsbeta \
        --prefix toolsbeta-test-k8s-etcd \
        --fqdn-to-add toolsbeta-test-k8s-etcd-09.toolsbeta.eqiad1.wikimedia.cloud

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import json
import logging
from typing import Optional

import yaml
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

LOGGER = logging.getLogger(__name__)


class AddNodeToHiera(CookbookBase):
    """WMCS Toolforge cookbook to add a new etcd node to hiera"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--prefix",
            required=False,
            default=None,
            help=("Prefix for etcd nodes in this project, will autogenerate by " "default (<project>-k8s-etcd)"),
        )
        parser.add_argument("--fqdn-to-add", required=True, help="FQDN of the node to add")

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get Runner"""
        return AddNodeToHieraRunner(
            fqdn_to_add=args.fqdn_to_add,
            prefix=args.prefix,
            project=args.project,
            spicerack=self.spicerack,
        )


class AddNodeToHieraRunner(CookbookRunnerBase):
    """Runner for AddNodeToHiera"""

    def __init__(
        self,
        fqdn_to_add: str,
        prefix: str,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.spicerack = spicerack
        self.project = project
        self.prefix = prefix
        self.fqdn_to_add = fqdn_to_add

    def run(self) -> Optional[int]:
        """Main entry point"""
        control_node = self.spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True)

        etcd_prefix = self.prefix if self.prefix is not None else f"{self.project}-k8s-etcd"
        response = yaml.safe_load(
            next(
                control_node.run_sync(
                    ("wmcs-enc-cli --openstack-project " + self.project + " get_prefix_hiera " + etcd_prefix),
                    is_safe=True,
                )
            )[1]
            .message()
            .decode()
        )
        current_hiera_config = yaml.safe_load(response["hiera"])
        changed = False

        nodes = current_hiera_config.get("profile::toolforge::k8s::etcd_nodes", [])
        if self.fqdn_to_add not in nodes:
            nodes.append(self.fqdn_to_add)
            changed = True

        current_hiera_config["profile::toolforge::k8s::etcd_nodes"] = nodes

        alt_names = current_hiera_config.get("profile::base::puppet::dns_alt_names", [])
        if self.fqdn_to_add not in alt_names:
            alt_names.append(self.fqdn_to_add)
            changed = True

        current_hiera_config["profile::base::puppet::dns_alt_names"] = alt_names

        if changed:
            # json is a one-line string, with only double quotes, nicer for
            # usage as cli parameter, and it's valid yaml :)
            current_hiera_config_str = json.dumps(current_hiera_config)
            LOGGER.info("New hiera config:\n%s", current_hiera_config_str)

            response = yaml.safe_load(
                next(
                    control_node.run_sync(
                        (
                            f"wmcs-enc-cli --openstack-project {self.project} "
                            f"set_prefix_hiera {etcd_prefix} '{current_hiera_config_str}'"
                        ),
                    )
                )[1]
                .message()
                .decode()
            )
        else:
            LOGGER.info("Hiera config was already correct.")

        return current_hiera_config