r"""Renew the puppet certificate of a single host

 * puppet cert clean the old certificate on the puppet master
 * delete the old certificate on the host
 * run puppet to generate a new certificate and the host
 * validate the puppet master see's the new certificate on the puppet master
 * sign the new certificate on the puppet master
 * run puppet on the host to ensure everything works as expected

Usage example:
    cookbook sre.hosts.renew-cert sretest1001.eqiad.wmnet
"""

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from datetime import timedelta
from logging import getLogger


__title__ = 'Renew the puppet certificate of a single host'
logger = getLogger(__name__)


def argument_parser():
    """Parse arguments"""
    parser = ArgumentParser(description=__doc__, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('host', help='A single host whose puppet certificate should be renewed')
    return parser


def run(args, spicerack):
    """Renew the certificate"""
    remote_host = spicerack.remote().query(args.host)

    if not remote_host:
        logger.error('Specified server not found, bailing out')
        return 1

    if len(remote_host) != 1:
        logger.error('Only a single server can be rebooted')
        return 1

    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_host)
    puppet_master = spicerack.puppet_master()
    reason = spicerack.admin_reason('Renew puppet certificate')
    with icinga.hosts_downtimed(remote_host.hosts, reason, duration=timedelta(minutes=20)):
        puppet_master.destroy(remote_host)
        puppet.disable(reason)
        fingerprints = puppet.regenerate_certificate()
        puppet_master.wait_for_csr(remote_host)
        puppet_master.sign(remote_host, fingerprints[remote_host])
        puppet.run(enable_reason=reason, quiet=True)

    return 0
