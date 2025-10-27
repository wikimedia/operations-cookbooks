"""Generic hosts Cookbooks"""

import logging
import re
import time

from dataclasses import dataclass

from wmflib.dns import DnsNotFound
from wmflib.interactive import ask_confirmation

from cumin.transports import Command

from spicerack.redfish import ChassisResetPolicy, RedfishError
from spicerack.remote import RemoteHosts


logger = logging.getLogger(__name__)
KERBEROS_KDC_KEYTAB_PATH = '/srv/kerberos/keytabs'
DEPLOYMENT_HOST = 'deployment.eqiad.wmnet'
MEDIAWIKI_CONFIG_REPO_PATH = '/srv/mediawiki-staging'
KERBEROS_KADMIN_CUMIN_ALIAS = 'A:kerberos-kadmin'
PUPPETSERVER_REPO_PATH = '/srv/git/operations/puppet'
PUPPETSERVER_PRIVATE_REPO_PATH = '/srv/git/private'
COMMON_STEPS_KEY = 'COMMON_STEPS'
DEPLOYMENT_CHARTS_REPO_PATH = '/srv/deployment-charts'
AUTHDNS_REPO_PATH = '/srv/authdns/git'

# Supported Debian OS versions
OS_VERSIONS = ('buster', 'bullseye', 'bookworm', 'trixie')

# Vendor slugs
DELL_VENDOR_SLUG = 'dell'
SUPERMICRO_VENDOR_SLUG = 'supermicro'
SUPPORTED_VENDORS = [DELL_VENDOR_SLUG, SUPERMICRO_VENDOR_SLUG]

# For per rack vlan migration
LEGACY_VLANS = (
    'private1-a-codfw',
    'private1-b-codfw',
    'private1-c-codfw',
    'private1-d-codfw'
)


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


def reboot_chassis(
        vendor: str, redfish_instance,
        chassis_reset_policy: ChassisResetPolicy = ChassisResetPolicy.GRACEFUL_RESTART,
        max_reboot_secs: int = 300):
    """Reboot chassis and poll or wait for completion

    Reboots the chassis and polls via Redfish for the completion of the
    reboot. If the polling fails it falls back to a set timeout.

    """
    if vendor not in SUPPORTED_VENDORS:
        raise RuntimeError(f"The vendor {vendor} is not supported.")

    redfish_instance.chassis_reset(chassis_reset_policy)
    logger.info('Waiting for chassis reboot to complete...')
    start_time = time.time()
    # TODO: Consider supporting older Supermicro models, which only return
    # the intermediate state 'None'
    pwr_states = {
        'supermicro': [
            'SystemHardwareInitializationComplete',
            'MemoryInitializationStarted',
        ],
        'dell': [
            'OSRunning',
            'SystemHardwareInitializationComplete',
        ]
    }
    # Watch for state transitions which signify the reboot is complete, we
    # don't just watch for the final state, since we want to ensure the
    # reboot occurred
    while len(pwr_states[vendor]) > 0:
        pwr_state = pwr_states[vendor].pop()
        while (time.time() - start_time) < max_reboot_secs:
            time.sleep(5)
            try:
                sys_prop = redfish_instance.request(
                    'GET',
                    redfish_instance.system_manager,
                ).json()
                logger.info(
                    'Reboot state: %s',
                    sys_prop['BootProgress']['LastState'],
                )
                if sys_prop['BootProgress']['LastState'] == pwr_state:
                    break
            except RedfishError as e:
                logger.error("Error while retrieving system properties: %s", e)
            except KeyError as e:
                logger.error(
                    "The BootProgress data is not present in Redfish. "
                    "Upgrading the BMC/BIOS firmwares should fix this, "
                    "please follow up with DCops or I/F if you have questions.\n"
                    "Error: %s", e)
    bios_start_time = time.time()
    # On Supermicro X13DDW-A the Redfish BIOS settings are not immediately
    # available, even though the system has booted, poll until they
    # available.
    while (time.time() - bios_start_time) < max_reboot_secs:
        try:
            redfish_instance.request(  # pylint: disable=expression-not-assigned
                'GET',
                f"{redfish_instance.system_manager}/Bios",
            ).json()['Attributes']
            break
        except (RedfishError, KeyError):
            logger.info('Reboot: Redfish BIOS settings not available, polling')
        time.sleep(5)
    logger.info('Chassis reboot completed, duration %d seconds', time.time() - start_time)
