"""Generic hosts Cookbooks"""

import logging
import re

from dataclasses import dataclass

from wmflib.dns import DnsNotFound
from wmflib.interactive import ask_confirmation

from cumin.transports import Command

from spicerack.remote import RemoteHosts


__title__ = __doc__

logger = logging.getLogger(__name__)
KERBEROS_KDC_KEYTAB_PATH = '/srv/kerberos/keytabs'
DEPLOYMENT_HOST = 'deployment.eqiad.wmnet'
MEDIAWIKI_CONFIG_REPO_PATH = '/srv/mediawiki-staging'
KERBEROS_KADMIN_CUMIN_ALIAS = 'A:kerberos-kadmin'
PUPPET_REPO_PATH = '/var/lib/git/operations/puppet'
PUPPET_PRIVATE_REPO_PATH = '/srv/private'
COMMON_STEPS_KEY = 'COMMON_STEPS'
DEPLOYMENT_CHARTS_REPO_PATH = '/srv/deployment-charts'
AUTHDNS_REPO_PATH = '/srv/authdns/git'

# Supported Debian OS versions
OS_VERSIONS = ('buster', 'bullseye', 'bookworm')


@dataclass(frozen=True)
class GitRepoPath:
    """Define a git repository to search for matches."""

    remote_host: RemoteHosts
    path: str
    pathspec: str = ''


def check_patterns_in_repo(repos: tuple[GitRepoPath, ...], patterns: list[str], interactive: bool = True) -> bool:
    """Git grep for all the given patterns in the given hosts and path and ask for confirmation if any is found.

    Arguments:
        repos: a sequence of GitRepoPath instances.
        patterns: a sequence of patterns to check.
        interactive: if the function should pause and ask the user confirmation to continue

    """
    grep_patterns = '|'.join(patterns)
    match_found = False
    for repo in repos:
        logger.info('Looking for matches in %s:%s %s', repo.remote_host, repo.path, repo.pathspec)
        grep_command = f"git -C '{repo.path}' grep -E '({grep_patterns})'"
        if repo.pathspec:
            grep_command += f" '{repo.pathspec}'"

        for _nodeset, _output in repo.remote_host.run_sync(Command(grep_command, ok_codes=[]), is_safe=True):
            match_found = True

    if match_found:
        message = (
            'Found match(es) in some git repositories for the target host '
            '(see above), verify they were not left by mistake before proceeding!')
        if interactive:
            ask_confirmation(message)
        else:
            logger.info(message)
    else:
        logger.info('No matches found for the target host in various git repositories')
    return match_found


def get_grep_patterns(dns, target_hosts, ip_only: bool = False):
    """Given a list of hostnames return the list of regex patterns for the hostname and all its IPs."""
    patterns = []
    for host in target_hosts:
        if not ip_only:
            patterns.append(re.escape(host))
        try:
            ips = dns.resolve_ips(host)
        except DnsNotFound:
            logger.warning('No DNS record found for host %s. Generating grep patterns for the name only', host)
            continue

        patterns.extend('[^0-9A-Za-z]{}[^0-9A-Za-z]'.format(re.escape(ip)) for ip in ips)

    return patterns


def find_kerberos_credentials(remote_host, target_hosts):
    """Check if any host provided has a kerberos keytab stored on the KDC hosts."""
    cred_found = False
    logger.info('Looking for Kerberos credentials on KDC kadmin node.')
    for host in target_hosts:
        find_keytabs_command = 'find {} -name "{}*"'.format(KERBEROS_KDC_KEYTAB_PATH, host)
        check_princs_command = '/usr/local/sbin/manage_principals.py list "*{}*"'.format(host)
        cumin_commands = [Command(find_keytabs_command, ok_codes=[]),
                          Command(check_princs_command, ok_codes=[])]
        for _nodeset, _output in remote_host.run_sync(*cumin_commands):
            cred_found = True

    if cred_found:
        logger.info('Please follow this guide to drop unused credentials: '
                    'https://wikitech.wikimedia.org/wiki/Analytics/Systems/Kerberos'
                    '#Delete_Kerberos_principals_and_keytabs_when_a_host_is_decommissioned')
    else:
        logger.info('No Kerberos credentials found.')
