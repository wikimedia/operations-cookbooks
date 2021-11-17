"""WMCS openstack - Safely reboot a cloudvirt node.

This icludes putting in maintenance, draining, and unsetting maintenance.

Usage example: wmcs.openstack.cloudvirt.safe_reboot \
    --control-node-fqdn cloudcontrol1003.wikimedia.org \
    --fqdn cloudvirt1013.eqiad.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from datetime import datetime
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackAPI, dologmsg
from cookbooks.wmcs.openstack.cloudvirt.drain import Drain
from cookbooks.wmcs.openstack.cloudvirt.unset_maintenance import UnsetMaintenance

LOGGER = logging.getLogger(__name__)


class SafeReboot(CookbookBase):
    """WMCS Openstack cookbook to safe reboot a cloudvirt node."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--control-node-fqdn",
            required=False,
            default="cloudcontrol1003.wikimedia.org",
            help="FQDN of the control node to orchestrate from.",
        )
        parser.add_argument(
            "--fqdn",
            required=True,
            help="FQDN of the cloudvirt to SafeReboot.",
        )
        parser.add_argument(
            "--task-id",
            required=False,
            default=None,
            help="Id of the task related to this reboot (ex. T123456)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return SafeRebootRunner(
            fqdn=args.fqdn,
            task_id=args.task_id,
            control_node_fqdn=args.control_node_fqdn,
            spicerack=self.spicerack,
        )


class SafeRebootRunner(CookbookRunnerBase):
    """Runner for SafeReboot"""

    def __init__(
        self,
        fqdn: str,
        control_node_fqdn: str,
        spicerack: Spicerack,
        task_id: Optional[str] = None,
    ):
        """Init"""
        self.fqdn = fqdn
        self.control_node_fqdn = control_node_fqdn
        self.spicerack = spicerack
        self.task_id = task_id
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn=control_node_fqdn,
        )

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(
            project="admin",
            message=f"Safe rebooting '{self.fqdn}'.",
            task_id=self.task_id,
        )
        drain_cookbook = Drain(spicerack=self.spicerack)
        drain_cookbook.get_runner(
            args=drain_cookbook.argument_parser().parse_args(
                args=[
                    "--control-node-fqdn",
                    self.control_node_fqdn,
                    "--fqdn",
                    self.fqdn,
                    "--task-id",
                    self.task_id,
                ],
            )
        ).run()

        remote_host = self.spicerack.remote().query(f"D{{{self.fqdn}}}", use_sudo=True)
        reboot_time = datetime.utcnow()
        LOGGER.info("Rebooting and waiting for %s up", remote_host)
        remote_host.reboot()
        remote_host.wait_reboot_since(reboot_time)

        unset_maintenance_cookbook = UnsetMaintenance(spicerack=self.spicerack)
        unset_maintenance_cookbook.get_runner(
            args=unset_maintenance_cookbook.argument_parser().parse_args(
                args=[
                    "--control-node-fqdn",
                    self.control_node_fqdn,
                    "--fqdn",
                    self.fqdn,
                    "--task-id",
                    self.task_id,
                ],
            )
        ).run()
        dologmsg(
            project="admin",
            message=f"Safe reboot of '{self.fqdn}' finished successfully.",
            task_id=self.task_id,
        )
