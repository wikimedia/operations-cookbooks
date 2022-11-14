r"""WMCS openstack - upgrade live (without stopping VMs or rebooting) a cloudvirt node in maintenance

Usage example: wmcs.openstack.cloudvirt.live_upgrade_openstack \
    --fqdn-to-upgrade cloudvirt1013.eqiad.wmnet

"""
import argparse
import logging

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import run_one_raw

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

    def run(self) -> None:
        """Main entry point."""
        node_to_upgrade = self.spicerack.remote().query(f"D{{{self.fqdn_to_upgrade}}}", use_sudo=True)
        run_one_raw(node=node_to_upgrade, command=["puppet", "agent", "--enable"])
        run_one_raw(node=node_to_upgrade, command=Command("run-puppet-agent", ok_codes=[]))
        run_one_raw(node=node_to_upgrade, command=["apt", "update"])
        run_one_raw(
            node=node_to_upgrade,
            command=[
                "DEBIAN_FRONTEND=noninteractive",
                "apt-get",
                "install",
                "-y",
                "python3-libvirt",
                "python3-os-vif",
                "nova-compute",
                "neutron-common",
                "nova-compute-kvm",
                "neutron-linuxbridge-agent",
                "python3-neutron ",
                "python3-eventlet",
                "python3-oslo.messaging",
                "python3-taskflow",
                "python3-tooz",
                "python3-keystoneauth1",
                "python3-requests",
                "python3-urllib3",
                "-o",
                '"Dpkg::Options::=--force-confdef"',
                "-o",
                '"Dpkg::Options::=--force-confold"',
            ],
        )
        run_one_raw(
            node=node_to_upgrade,
            command=[
                "DEBIAN_FRONTEND=noninteractive",
                "apt-get",
                "dist-upgrade",
                "-y",
                "--allow-downgrades",
                "-o",
                '"Dpkg::Options::=--force-confdef"',
                "-o",
                '"Dpkg::Options::=--force-confold"',
            ],
        )
        run_one_raw(node=node_to_upgrade, command=Command("run-puppet-agent", ok_codes=[]))
        run_one_raw(node=node_to_upgrade, command=["systemctl", "restart", "neutron-linuxbridge-agent"])
        run_one_raw(node=node_to_upgrade, command=["systemctl", "stop", "libvirtd"])
        run_one_raw(node=node_to_upgrade, command=["systemctl", "start", "libvirtd-tls.socket"])
        run_one_raw(node=node_to_upgrade, command=["systemctl", "start", "libvirtd"])
        run_one_raw(node=node_to_upgrade, command=["run-puppet-agent"])
        run_one_raw(node=node_to_upgrade, command=["systemctl", "restart", "nova-compute"])
        run_one_raw(node=node_to_upgrade, command=["journalctl", "-n", "500"])
        LOGGER.info(
            "Those were the last lines of the journal, make sure everything looks ok before upgrading the next host."
        )
        LOGGER.info("%s Done!!! \\o/", self.fqdn_to_upgrade)
