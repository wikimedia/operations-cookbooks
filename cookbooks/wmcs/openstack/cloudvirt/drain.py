"""WMCS openstack - Drain a cloudvirt node

Usage example: wmcs.openstack.cloudvirt.drain \
    --control-node-fqdn cloudcontrol1003.eqiad.wmnet
    --fqdn cloudvirt1013.eqiad.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackAPI
from cookbooks.wmcs.openstack.cloudvirt.set_maintenance import SetMaintenance

LOGGER = logging.getLogger(__name__)


class Drain(CookbookBase):
    """WMCS Openstack cookbook to drain a cloudvirt node."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--control-node-fqdn",
            required=True,
            help="FQDN of the control node to orchestrate from.",
        )
        parser.add_argument(
            "--fqdn",
            required=True,
            help="FQDN of the cloudvirt to drain.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return DrainRunner(
            fqdn=args.fqdn,
            control_node_fqdn=args.control_node_fqdn,
            spicerack=self.spicerack,
        )


class DrainRunner(CookbookRunnerBase):
    """Runner for Drain"""

    def __init__(
        self,
        fqdn: str,
        control_node_fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.fqdn = fqdn
        self.control_node_fqdn = control_node_fqdn
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn=control_node_fqdn,
        )

    def run(self) -> Optional[int]:
        """Main entry point"""
        set_maintenance_cookbook = SetMaintenance(spicerack=self.spicerack)
        set_maintenance_cookbook.get_runner(
            args=set_maintenance_cookbook.argument_parser().parse_args(
                args=[
                    "--control-node-fqdn",
                    self.control_node_fqdn,
                    "--fqdn",
                    self.fqdn,
                ],
            )
        ).run()
        hypervisor_name = self.fqdn.split('.', 1)[0]
        self.openstack_api.drain_hypervisor(hypervisor_name=hypervisor_name)