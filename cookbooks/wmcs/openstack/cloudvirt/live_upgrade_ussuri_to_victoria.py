"""WMCS openstack - upgrade live (without stopping VMs or rebooting) a cloudvirt node in maintenance

Usage example: wmcs.openstack.cloudvirt.live_upgrade_ussuri_to_victoria \
    --fqdn-to-upgrade cloudvirt1013.eqiad.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

LOGGER = logging.getLogger(__name__)


class LiveUpgrade(CookbookBase):
    """WMCS Openstack cookbook to set a cloudvirt node in maintenance."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--fqdn-to-upgrade",
            required=True,
            help="FQDN of the cloudvirt to set in maintenance.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return LiveUpgradeRunner(
            fqdn_to_upgrade=args.fqdn_to_upgrade,
            spicerack=self.spicerack,
        )


class LiveUpgradeRunner(CookbookRunnerBase):
    """Runner for LiveUpgrade."""

    def __init__(
        self,
        fqdn_to_upgrade: str,
        spicerack: Spicerack,
    ):
        """Init."""
        self.fqdn_to_upgrade = fqdn_to_upgrade
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point."""
        node_to_upgrade = self.spicerack.remote().query(f"D{{{self.fqdn_to_upgrade}}}", use_sudo=True)
        input(f"Start with {self.fqdn_to_upgrade}?")
        node_to_upgrade.run_async("puppet agent --enable")
        node_to_upgrade.run_sync(Command("run-puppet-agent", ok_codes=list(range(255))))
        node_to_upgrade.run_sync("apt update")
        node_to_upgrade.run_sync(
            "DEBIAN_FRONTEND=noninteractive apt-get install -y python3-libvirt python3-os-vif nova-compute "
            "neutron-common nova-compute-kvm neutron-linuxbridge-agent python3-neutron  python3-eventlet "
            "python3-oslo.messaging python3-taskflow python3-tooz python3-keystoneauth1 python3-positional "
            "python3-requests python3-urllib3 "
            '-o "Dpkg::Options::=--force-confdef" '
            '-o "Dpkg::Options::=--force-confold"'
        )
        node_to_upgrade.run_sync(
            "DEBIAN_FRONTEND=noninteractive apt-get dist-upgrade -y --allow-downgrades "
            '-o "Dpkg::Options::=--force-confdef" '
            '-o "Dpkg::Options::=--force-confold"'
        )
        node_to_upgrade.run_sync(Command("run-puppet-agent", ok_codes=list(range(255))))
        node_to_upgrade.run_sync("systemctl restart neutron-linuxbridge-agent")
        node_to_upgrade.run_sync("systemctl stop libvirtd")
        node_to_upgrade.run_sync("systemctl start libvirtd-tls.socket")
        node_to_upgrade.run_sync("systemctl start libvirtd")
        node_to_upgrade.run_sync("run-puppet-agent")
        node_to_upgrade.run_sync("systemctl restart nova-compute")
        node_to_upgrade.run_sync("journalctl -n 500")
        LOGGER.info(
            "Those were the last lines of the journal, make sure everyting looks ok before upgrading the next host."
        )
        LOGGER.info(f"{self.fqdn_to_upgrade} Done!!! \\o/")
