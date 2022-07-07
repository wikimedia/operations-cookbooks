"""WMCS Ceph - Generic cookbook to upgrade a ceph node.

Usage example:
    cookbook wmcs.ceph.upgrade_ceph_node \
        --to-upgrade-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
import argparse
import datetime
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.lib.alerts import downtime_host, uptime_host
from cookbooks.wmcs.lib.ceph import CephClusterController, CephOSDFlag

LOGGER = logging.getLogger(__name__)


class UpgradeCephNode(CookbookBase):
    """WMCS Ceph cookbook to upgrade a node."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--to-upgrade-fqdn",
            required=True,
            help="FQDN of the node to upgrade",
        )
        parser.add_argument(
            "--skip-maintenance",
            required=False,
            action="store_true",
            help="If set, will not put the cluster into maintenance nor take it out of it.",
        )
        parser.add_argument(
            "--force",
            required=False,
            action="store_true",
            help="If passed, will continue even if the cluster is not in a healthy state.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return UpgradeCephNodeRunner(
            to_upgrade_fqdn=args.to_upgrade_fqdn,
            skip_maintenance=args.skip_maintenance,
            force=args.force,
            spicerack=self.spicerack,
        )


class UpgradeCephNodeRunner(CookbookRunnerBase):
    """Runner for UpgradeCephNode"""

    def __init__(
        self,
        to_upgrade_fqdn: str,
        skip_maintenance: bool,
        force: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.to_upgrade_fqdn = to_upgrade_fqdn
        self.force = force
        self.skip_maintenance = skip_maintenance
        self.spicerack = spicerack

    def run(self) -> None:
        """Main entry point"""
        LOGGER.info("Upgrading ceph node %s", self.to_upgrade_fqdn)
        controller = CephClusterController(
            remote=self.spicerack.remote(), controlling_node_fqdn=self.to_upgrade_fqdn, spicerack=self.spicerack
        )
        # make sure we make cluster info commands on another node
        controller.change_controlling_node()

        if not self.skip_maintenance:
            silences = controller.set_maintenance(
                force=self.force, reason=f"Upgrading the ceph node {self.to_upgrade_fqdn}."
            )

        # Can't use sre upgrade-and-reboot due to it using alertmanager's api to downtime hosts
        remote_host = self.spicerack.remote().query(self.to_upgrade_fqdn, use_sudo=True)
        host_name = self.to_upgrade_fqdn.split(".", 1)[0]
        puppet = self.spicerack.puppet(remote_host)
        downtime_id = downtime_host(
            spicerack=self.spicerack,
            host_name=host_name,
            comment="Ceph node software upgrade and reboot",
            duration="20m",
        )
        puppet_reason = self.spicerack.admin_reason("Software upgrade and reboot")

        with puppet.disabled(puppet_reason):
            # Upgrade all packages, leave config files untouched, do not prompt
            upgrade_cmd = (
                "DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::='--force-confdef' "
                "-o Dpkg::Options::='--force-confold' dist-upgrade"
            )
            remote_host.run_sync(upgrade_cmd)

            reboot_time = datetime.datetime.utcnow()
            remote_host.reboot()
            remote_host.wait_reboot_since(reboot_time)

        puppet.run()

        uptime_host(spicerack=self.spicerack, host_name=host_name, silence_id=downtime_id)

        # Once the node is up, let it rebalance
        controller.unset_osdmap_flag(CephOSDFlag("norebalance"))
        controller.wait_for_cluster_healthy(consider_maintenance_healthy=True, timeout_seconds=300)
        controller.set_osdmap_flag(CephOSDFlag("norebalance"))

        if not self.skip_maintenance:
            controller.unset_maintenance(force=self.force, silences=silences)
