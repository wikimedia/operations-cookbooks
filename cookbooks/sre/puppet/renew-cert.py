"""Renew the puppet certificate of a single host"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import timedelta
from logging import getLogger

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

logger = getLogger(__name__)


class RenewCert(CookbookBase):
    """Renew the puppet certificate of a single host

    * puppet cert clean the old certificate on the puppet master
    * delete the old certificate on the host
    * run puppet to generate a new certificate and the host
    * validate the puppet master see's the new certificate on the puppet master
    * sign the new certificate on the puppet master
    * run puppet on the host to ensure everything works as expected

    Usage example:
        cookbook sre.hosts.renew-cert sretest1001.eqiad.wmnet
    """

    def argument_parser(self):
        """Parse arguments"""
        parser = ArgumentParser(description=self.__doc__, formatter_class=RawDescriptionHelpFormatter)
        parser.add_argument('query', help='A single host whose puppet certificate should be renewed')
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RenewCertRunner(args, self.spicerack)


class RenewCertRunner(CookbookRunnerBase):
    """renew-cert cookbook runner"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        hosts = spicerack.remote().query(args.host)

        if not hosts:
            raise RuntimeError('No host found for query "{query}"'.format(query=args.query))

        if len(hosts) != 1:
            raise RuntimeError('Only a single server can be rebooted')

        self.host = str(hosts.hosts[0])
        self.icinga = spicerack.icinga()
        self.puppet = spicerack.puppet([self.host])
        self.puppet_master = spicerack.puppet_master()
        self.reason = spicerack.admin_reason('Renew puppet certificate')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for {s.host}: {s.reason}'.format(s=self)

    def run(self):
        """Renew the certificate"""
        with self.icinga.hosts_downtimed([self.host], self.reason, duration=timedelta(minutes=20)):
            self.puppet_master.destroy(self.host)
            self.puppet.disable(self.reason)
            fingerprints = self.puppet.regenerate_certificate()
            self.puppet_master.wait_for_csr(self.host)
            self.puppet_master.sign(self.host, fingerprints[self.host])
            self.puppet.run(enable_reason=self.reason, quiet=True)
