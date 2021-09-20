"""WMCS VPS - Remove and regenerate the puppet certificates of the host.

Usage example: wmcs.vps.refresh_puppet_certs \
    --fqdn tools-host.tools.eqiad1.wikimedia.cloud

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts, PuppetMaster

LOGGER = logging.getLogger(__name__)


class RefreshPuppetCerts(CookbookBase):
    """WMCS VPS cookbook to bootstrap puppet on a node that uses a project puppetmaster."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument(
            "--fqdn",
            required=True,
            help="FQDN of the to bootstrap (ex. toolsbeta-test-k8s-etcd-9.toolsbeta.eqiad1.wikimedia.cloud)",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return RefreshPuppetCertsRunner(
            fqdn=args.fqdn,
            spicerack=self.spicerack,
        )


class RefreshPuppetCertsRunner(CookbookRunnerBase):
    """Runner for RefreshPuppetCerts"""

    def __init__(
        self,
        fqdn: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.fqdn = fqdn
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        node_to_bootsrap = PuppetHosts(
            remote_hosts=self.spicerack.remote().query(
                f"D{{{self.fqdn}}}",
                use_sudo=True,
            ),
        )
        puppetmasters = node_to_bootsrap.get_ca_servers()
        puppetmaster_fqdn = puppetmasters[self.fqdn]
        puppetmaster = PuppetMaster(
            master_host=self.spicerack.remote().query(
                f"D{{{puppetmaster_fqdn}}}",
                use_sudo=True,
            )
        )

        puppetmaster.destroy(hostname=self.fqdn)

        cert_fingerprint = node_to_bootsrap.regenerate_certificate()[self.fqdn]
        puppetmaster.sign(
            hostname=self.fqdn,
            fingerprint=cert_fingerprint,
            allow_alt_names=True,
        )
        node_to_bootsrap.run()
