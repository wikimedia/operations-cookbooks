r"""WMCS VPS - Remove and regenerate the puppet certificates of the host.

Usage example: wmcs.vps.refresh_puppet_certs \
    --fqdn tools-host.tools.eqiad1.wikimedia.cloud

"""
import argparse
import logging

from cumin.transports import Command
from spicerack import RemoteHosts, Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts, PuppetMaster
from spicerack.remote import RemoteExecutionError

from cookbooks.wmcs.libs.common import run_one_raw

LOGGER = logging.getLogger(__name__)


def _get_puppetmaster(spicerack: Spicerack, remote_host: RemoteHosts, puppetmaster: str) -> PuppetMaster:
    puppetmaster_fqdn = puppetmaster
    if puppetmaster_fqdn == "puppet":
        puppetmaster_fqdn = run_one_raw(
            node=remote_host, command=["dig", "+short", "-x", "$(dig +short puppet)"]
        ).strip()
        # remove the extra dot that dig appends
        puppetmaster_fqdn = puppetmaster_fqdn[:-1]

    return PuppetMaster(
        master_host=spicerack.remote().query(
            f"D{{{puppetmaster_fqdn}}}",
            use_sudo=True,
        )
    )


def _refresh_cert(
    spicerack: Spicerack,
    remote_host: RemoteHosts,
) -> None:
    """Takes care of the dance to remove and regenerate a cert on the host and it's puppetmaster."""
    node_to_bootstrap = PuppetHosts(remote_hosts=remote_host)
    fqdn = str(remote_host)
    puppetmasters = node_to_bootstrap.get_ca_servers()
    puppetmaster = _get_puppetmaster(
        spicerack=spicerack,
        remote_host=remote_host,
        puppetmaster=puppetmasters[fqdn],
    )
    puppetmaster.destroy(hostname=fqdn)
    cert_fingerprint = node_to_bootstrap.regenerate_certificate()[fqdn]
    cert = puppetmaster.get_certificate_metadata(hostname=fqdn)
    if cert["state"] == PuppetMaster.PUPPET_CERT_STATE_SIGNED:
        # the cert exists and is already signed
        return

    puppetmaster.sign(
        hostname=fqdn,
        fingerprint=cert_fingerprint,
        allow_alt_names=True,
    )


class RefreshPuppetCerts(CookbookBase):
    """WMCS VPS cookbook to bootstrap puppet on a node that uses a project puppetmaster."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--fqdn",
            required=True,
            help="FQDN of the to bootstrap (ex. toolsbeta-test-k8s-etcd-9.toolsbeta.eqiad1.wikimedia.cloud)",
        )
        parser.add_argument(
            "--pre-run-puppet",
            action="store_true",
            help="If passed, will force a puppet run (ignoring the results) before refreshing the certs.",
        )
        parser.add_argument(
            "--ignore-failures",
            action="store_true",
            help="If passed, will ignore any failures when running puppet.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return RefreshPuppetCertsRunner(
            fqdn=args.fqdn,
            pre_run_puppet=args.pre_run_puppet,
            ignore_failures=args.ignore_failures,
            spicerack=self.spicerack,
        )


class RefreshPuppetCertsRunner(CookbookRunnerBase):
    """Runner for RefreshPuppetCerts"""

    def __init__(
        self,
        fqdn: str,
        pre_run_puppet: bool,
        ignore_failures: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.fqdn = fqdn
        self.pre_run_puppet = pre_run_puppet
        self.ignore_failures = ignore_failures
        self.spicerack = spicerack

    def run(self) -> None:
        """Main entry point.

        Basic process:
            Refresh certs on current puppetmaster (in case the fqdn already existed)
            Try to run puppet (pulls new puppetmaster if needed, might fail)
            If there's new puppetmasters, refresh certs on those
            If there was new puppetmasters or the first puppet run failed, run puppet again
        """
        remote_host = self.spicerack.remote().query(f"D{{{self.fqdn}}}", use_sudo=True)
        node_to_bootstrap = PuppetHosts(remote_hosts=remote_host)
        pre_run_passed = False

        # For the first run, make sure that the current master has no cert with this fqdn
        pre_puppetmasters = node_to_bootstrap.get_ca_servers()
        _refresh_cert(spicerack=self.spicerack, remote_host=remote_host)

        if self.pre_run_puppet:
            try:
                node_to_bootstrap.run()
                pre_run_passed = True
            except RemoteExecutionError:
                if self.ignore_failures:
                    pass
                else:
                    raise

        else:
            # We have to make sure in any case that the puppet config is refreshed to do the puppetmaster switch.
            # The tag makes only run the puppet config related manifests.
            run_one_raw(node=remote_host, command=Command("puppet agent --test --tags base::puppet", ok_codes=[]))

        post_puppetmasters = node_to_bootstrap.get_ca_servers()
        if post_puppetmasters != pre_puppetmasters:
            _refresh_cert(spicerack=self.spicerack, remote_host=remote_host)

        if post_puppetmasters == pre_puppetmasters or not pre_run_passed:
            try:
                node_to_bootstrap.run()
            except RemoteExecutionError:
                if self.ignore_failures:
                    pass
                else:
                    raise
