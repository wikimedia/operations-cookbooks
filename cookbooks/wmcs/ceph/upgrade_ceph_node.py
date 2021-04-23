"""WMCS Ceph - Generic cookbook to upgrade a ceph node.

Usage example:
    cookbook wmcs.ceph.upgrade_ceph_node \
        --to-upgrade-fqdn cloudcephosd2001-dev.codfw.wmnet

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import importlib
import logging
from typing import Optional

from spicerack import ICINGA_DOMAIN, Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteHosts

from cookbooks.wmcs import CephController

# Ugly hack to work around the fact that the module has a non-valid identifier
# file name
upgrade_and_reboot = importlib.import_module("cookbooks.sre.hosts.upgrade-and-reboot")
LOGGER = logging.getLogger(__name__)


class UpgradeCephNode(CookbookBase):
    """WMCS Ceph cookbook to upgrade a node."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
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

    def run(self) -> Optional[int]:
        """Main entry point"""
        LOGGER.info("Upgrading ceph node %s", self.to_upgrade_fqdn)
        controller = CephController(remote=self.spicerack.remote(), controlling_node_fqdn=self.to_upgrade_fqdn)
        # make sure we make cluster info commands on another node
        controller.change_controlling_node()

        if not self.skip_maintenance:
            controller.set_maintenance(force=self.force)

        # we have to patch the master host to allow sudo, all this weirdness is
        # because icinga_master_host is an @property and can't be patched on
        # the original instance
        my_spicerack = self.spicerack

        class SudoIcingaSpicerackWrapper(Spicerack):
            """Dummy wrapper class to allow sudo icinga."""

            def __init__(self):  # pylint: disable-msg=super-init-not-called
                """Init."""

            @property
            def icinga_master_host(self) -> RemoteHosts:
                """Icinga master host."""
                new_host = self.remote().query(
                    query_string=self.dns().resolve_cname(ICINGA_DOMAIN),
                    use_sudo=True,
                )
                return new_host

            def __getattr__(self, what):
                return getattr(my_spicerack, what)

            def __setattr__(self, what, value):
                return setattr(my_spicerack, what, value)

        upgrade_and_reboot.run(
            args=upgrade_and_reboot.argument_parser().parse_args(
                [
                    self.to_upgrade_fqdn,
                    "--depool-cmd",
                    "true",
                    "--repool-cmd",
                    "true",
                    "--sleep",
                    "0",
                    "--use-sudo",
                ]
            ),
            spicerack=SudoIcingaSpicerackWrapper(),
        )

        controller.wait_for_cluster_healthy(consider_maintenance_healthy=True, timeout_seconds=300)

        if not self.skip_maintenance:
            controller.unset_maintenance(force=self.force)
