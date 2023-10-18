"""Renew the puppet certificate of a single host"""
from datetime import timedelta
from logging import getLogger

from packaging.version import Version
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre.puppet import get_puppet_version

logger = getLogger(__name__)


class RenewCert(CookbookBase):
    """Renew the puppet certificate of a single host

    * puppet cert clean the old certificate on the puppet master
    * delete the old certificate on the host
    * run puppet to generate a new certificate and the host
    * validate the puppet master see's the new certificate on the puppet master
    * sign the new certificate on the puppet master
    * run puppet on the host to ensure everything works as expected
    * optionally allow for alternative names in the Puppet certificate
    * optionally use the installer key for hosts upgraded in place instead of reimaged

    Usage example:
        cookbook sre.hosts.renew-cert sretest1001.eqiad.wmnet
        cookbook sre.hosts.renew-cert --installer sretest1001.eqiad.wmnet
    """

    def argument_parser(self):
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument('query', help='A single host whose puppet certificate should be renewed')
        parser.add_argument('--installer', action='store_true',
                            help=('To use the installer SSH key to connect to the host, the one set by the Debian '
                                  'installer and valid until the first Puppet run. Needed for example when '
                                  'reinstalling in place instead of using the reimage cookbook.'))
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RenewCertRunner(args, self.spicerack)


class RenewCertRunner(CookbookRunnerBase):
    """renew-cert cookbook runner"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.installer = args.installer

        hosts = spicerack.remote(installer=self.installer).query(args.query)
        if not hosts:
            raise RuntimeError(f'No host found for query "{args.query}"')

        if len(hosts) != 1:
            raise RuntimeError(f'Only a single server should match the query, got {len(hosts)}')

        self.host = str(hosts.hosts[0])
        self.spicerack = spicerack
        self.session = spicerack.requests_session(__name__)
        self.alerting_hosts = spicerack.alerting_hosts(hosts.hosts)
        self.puppet = spicerack.puppet(hosts)
        self.puppet_master = self._get_puppet_server()
        self.reason = spicerack.admin_reason('Renew puppet certificate')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for {self.host}: {self.reason}'

    def _get_puppet_server(self):
        """Return the correct class based on the target host puppet version."""
        puppet_version = get_puppet_version(self.session, self.host.split('.', maxsplit=1)[0])
        if puppet_version is None:
            raise RuntimeError(f"Unable to get puppet version for {self.host}")
        if puppet_version < Version("7"):
            return self.spicerack.puppet_master()
        return self.spicerack.puppet_server()

    def run(self):
        """Renew the certificate"""
        if self.installer:
            self._run()
        else:
            with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
                self._run()

    def _run(self):
        """Run all the actual steps to renew the certificate."""
        self.puppet_master.destroy(self.host)
        if not self.installer:  # On a freshly reinstalled system we don't have disable-puppet yet
            self.puppet.disable(self.reason)
        fingerprints = self.puppet.regenerate_certificate()
        self.puppet_master.wait_for_csr(self.host)
        self.puppet_master.sign(self.host, fingerprints[self.host])
        if not self.installer:
            self.puppet.run(enable_reason=self.reason, quiet=True)
