"""Decommission a host from all inventories."""
import argparse
import logging
import re
import subprocess
import time

from cumin.transports import Command
from pynetbox.core.query import RequestError
from wmflib.dns import DnsError, DnsNotFound
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.ipmi import IpmiError
from spicerack.puppet import get_puppet_ca_hostname
from spicerack.remote import NodeSet, RemoteError, RemoteExecutionError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.dns.netbox import argument_parser as dns_netbox_argparse, run as dns_netbox_run


logger = logging.getLogger(__name__)
DEPLOYMENT_HOST = 'deployment.eqiad.wmnet'
MEDIAWIKI_CONFIG_REPO_PATH = '/srv/mediawiki-staging'
KERBEROS_KDC_KEYTAB_PATH = '/srv/kerberos/keytabs'
KERBEROS_KADMIN_CUMIN_ALIAS = 'A:kerberos-kadmin'
PUPPET_REPO_PATH = '/var/lib/git/operations/puppet'
PUPPET_PRIVATE_REPO_PATH = '/srv/private'
COMMON_STEPS_KEY = 'COMMON_STEPS'


def check_patterns_in_repo(host_paths, patterns):
    """Git grep for all the given patterns in the given hosts and path and ask for confirmation if any is found.

    Arguments:
        host_paths (sequence): a sequence of 2-item tuples with the RemoteHost instance and the path of the
            repositories to check.
        patterns (sequence): a sequence of patterns to check.

    """
    grep_command = "git grep -E '({patterns})'".format(patterns='|'.join(patterns))
    ask = False
    for remote_host, path in host_paths:
        logger.info('Looking for matches in %s:%s', remote_host, path)
        command = 'cd {path} && {grep}'.format(path=path, grep=grep_command)
        for _nodeset, _output in remote_host.run_sync(Command(command, ok_codes=[])):
            ask = True

    if ask:
        ask_confirmation(
            'Found match(es) in the Puppet or mediawiki-config repositories '
            '(see above), proceed anyway?')
    else:
        logger.info('No matches found in the Puppet or mediawiki-config repositories')


def get_grep_patterns(dns, decom_hosts):
    """Given a list of hostnames return the list of regex patterns for the hostname and all its IPs."""
    patterns = []
    for host in decom_hosts:
        patterns.append(re.escape(host))
        try:
            ips = dns.resolve_ips(host)
        except DnsNotFound:
            logger.warning('No DNS record found for host %s. Generating grep patterns for the name only', host)
            continue

        patterns.extend('[^0-9A-Za-z]{}[^0-9A-Za-z]'.format(re.escape(ip)) for ip in ips)

    return patterns


def find_kerberos_credentials(remote_host, decom_hosts):
    """Check if any host provided has a kerberos keytab stored on the KDC hosts."""
    cred_found = False
    logger.info('Looking for Kerberos credentials on KDC kadmin node.')
    for host in decom_hosts:
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


# Temporary workaround as Netbox sometimes fails with 500 when updating the device because of a stale reference
# to one of the primary IPs.
@retry(tries=4, exceptions=(RequestError,))
def update_netbox(netbox, netbox_data, dry_run):
    """Delete all non-mgmt IPs, disable remote interfaces/vlan and set the status to Decommissioning."""
    # TODO: this is needed instead of calling put_hosts_status() because Netbox cache doesn't get invalidated
    #       immediately making the call to fail. A sleep of 10 seconds did not fix the issue either.
    device = netbox.api.dcim.devices.get(netbox_data['id'])
    device.primary_ip4_id = None
    device.primary_ip6_id = None
    device.status = 'decommissioning'
    if not dry_run:
        device.save()

    switches = set()
    for interface in netbox.api.dcim.interfaces.filter(device_id=netbox_data['id']):
        if interface.mgmt_only:  # Ignore mgmt interfaces
            logger.debug('Skipping interface %s, mgmt_only=True', interface.name)
            continue
        # If the interface is connected to another interface (and not a circuit, etc)
        if interface.connected_endpoint and interface.connected_endpoint_type == 'dcim.interface':
            remote_interface = netbox.api.dcim.interfaces.get(interface.connected_endpoint.id)
            # Disable the remote side and reset any potential vlan config
            remote_interface.enabled = False
            remote_interface.mode = None
            remote_interface.untagged_vlan = None
            remote_interface.mtu = None
            remote_interface.tagged_vlans = []
            logger.info('Disable and reset vlan on %s:%s for local %s',
                        remote_interface.device.name, remote_interface.name, interface.name)
            if not dry_run:
                remote_interface.save()

            # Get the switch FQDN
            if remote_interface.device.virtual_chassis:
                switches.add(remote_interface.device.virtual_chassis.name)
            else:
                switches.add(remote_interface.device.primary_ip.dns_name)

        else:
            logger.debug('Interface %s is not connected to an interface', interface.name)
        # Remote is done, now we tackle the IPs
        if interface.count_ipaddresses > 0:
            for ip in netbox.api.ipam.ip_addresses.filter(interface_id=interface.id):
                logger.info('Delete IP %s on %s', ip.address, ip.assigned_object.name)
                if not dry_run:
                    ip.delete()
        else:
            logger.debug('No IPs on interface %s', interface.name)

    return switches


class DecommissionHost(CookbookBase):
    """Decommission a host from all inventories.

    It works for both Physical and Virtual hosts.
    If the query doesn't match any hosts allow to proceed with hostname expansion.

    List of actions performed on each host:
    - Check if any reference was left in the Puppet (both public and private) or
      mediawiki-config repositories and ask for confirmation before proceeding
      if there is any match.
    - Downtime the host on Icinga (it will be removed at the next Puppet run on
      the Icinga host).
    - Detect if Physical or Virtual host based on Netbox data.
    - If virtual host (Ganeti VM)
      - Ganeti shutdown (tries OS shutdown first, pulls the plug after 2 minutes)
      - Force Ganeti->Netbox sync of VMs to update its state and avoid
        Netbox Report errors
    - If physical host
      - Downtime the management host on Icinga (it will be removed at the next
        Puppet run on the Icinga host)
      - Wipe bootloaders to prevent it from booting again
      - Pull the plug (IPMI power off without shutdown)
      - Update Netbox state to Decommissioning and delete all non-mgmt interfaces
        and related IPs
    - Remove it from DebMonitor
    - Remove it from Puppet master and PuppetDB
    - If virtual host (Ganeti VM), issue a VM removal that will destroy the VM.
      Can take few minutes.
    - Run the sre.dns.netbox cookbook if the DC DNS records have been migrated
      to the automated system or tell the user that a manual patch is required.
    - Update the related Phabricator task

    Usage example:
        cookbook sre.hosts.decommission -t T12345 mw1234.codfw.wmnet

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
        parser.add_argument('query', help=('Cumin query to match the host(s) to act upon. At most 5 at a time, with '
                                           '--force at most 20 at a time.'))
        parser.add_argument('-t', '--task-id', required=True, help='the Phabricator task ID (e.g. T12345)')
        parser.add_argument('--force', action='store_true',
                            help='Bypass the default limit of 5 hosts at a time, but only up to 20 hosts.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return DecommissionHostRunner(args, self.spicerack)


class DecommissionHostRunner(CookbookRunnerBase):
    """Decommission host runner."""

    def __init__(self, args, spicerack):
        """Decommission a host from all inventories."""
        ensure_shell_is_durable()
        self.remote = spicerack.remote()
        try:
            self.decom_hosts = self.remote.query(args.query).hosts
        except RemoteError:
            logger.debug("Query '%s' did not match any host or failed", args.query, exc_info=True)
            decom_hosts = NodeSet(args.query)
            ask_confirmation(
                'ATTENTION: the query does not match any host in PuppetDB or failed\n'
                'Hostname expansion matches {n} hosts: {hosts}\n'
                'Do you want to proceed anyway?'
                .format(n=len(decom_hosts), hosts=decom_hosts))
            self.decom_hosts = decom_hosts

        if len(self.decom_hosts) > 20:
            raise RuntimeError(
                'Matched {} hosts, aborting. (max 20 with --force, 5 without)'
                .format(len(self.decom_hosts)))

        if len(self.decom_hosts) > 5:
            if args.force:
                logger.info('Authorized decommisioning of %s hosts with --force', len(self.decom_hosts))
            else:
                raise RuntimeError(
                    'Matched {} hosts, and --force not set aborting. (max 20 with --force, 5 without)'
                    .format(len(self.decom_hosts)))

        ask_confirmation(
            'ATTENTION: destructive action for {n} hosts: {hosts}\nAre you sure to proceed?'
            .format(n=len(self.decom_hosts), hosts=self.decom_hosts))

        self.spicerack = spicerack
        self.task_id = args.task_id
        self.puppet_master = self.remote.query(get_puppet_ca_hostname())
        self.kerberos_kadmin = self.remote.query(KERBEROS_KADMIN_CUMIN_ALIAS)
        self.dns = self.spicerack.dns()
        self.deployment_host = self.remote.query(self.dns.resolve_cname(DEPLOYMENT_HOST))
        self.patterns = get_grep_patterns(self.dns, self.decom_hosts)
        self.reason = self.spicerack.admin_reason('Host decommission', task_id=self.task_id)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for hosts {}'.format(self.decom_hosts)

    def _decommission_host(self, fqdn):  # noqa: MC0001
        """Perform all the decommissioning actions on a single host and return its switch if physical."""
        hostname = fqdn.split('.')[0]
        icinga = self.spicerack.icinga()
        puppet_master = self.spicerack.puppet_master()
        debmonitor = self.spicerack.debmonitor()
        netbox = self.spicerack.netbox(read_write=True)
        ganeti = self.spicerack.ganeti()
        switches = set()

        # Using the Direct Cumin backend to support also hosts already removed from PuppetDB
        remote_host = self.remote.query('D{' + fqdn + '}')

        # Downtime on Icinga both the host and the mgmt host (later below), they will be removed by Puppet
        try:
            icinga.downtime_hosts([fqdn], self.reason)
            self.spicerack.actions[fqdn].success('Downtimed host on Icinga')
        except RemoteExecutionError:
            self.spicerack.actions[fqdn].warning(
                '**Failed downtime host on Icinga (likely already removed)**')

        netbox_data = netbox.fetch_host_detail(hostname)
        is_virtual = netbox_data['is_virtual']
        if is_virtual:
            virtual_machine = ganeti.instance(fqdn, cluster=netbox_data['ganeti_cluster'])
            self.spicerack.actions[fqdn].success('Found Ganeti VM')
        else:
            ipmi = self.spicerack.ipmi(cached=True)
            mgmt = self.spicerack.management().get_fqdn(fqdn)
            self.spicerack.actions[fqdn].success('Found physical host')

        if is_virtual:
            try:
                virtual_machine.shutdown()
                self.spicerack.actions[fqdn].success('VM shutdown')
            except RemoteExecutionError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Failed to shutdown VM, manually run gnt-instance remove on the Ganeti '
                    'master for the {cluster} cluster**: {e}'
                    .format(cluster=virtual_machine.cluster, e=e))

            self.sync_ganeti(fqdn, virtual_machine)

        else:  # Physical host
            try:
                icinga.downtime_hosts([mgmt], self.reason)
                self.spicerack.actions[fqdn].success(
                    'Downtimed management interface on Icinga')
            except RemoteExecutionError:
                self.spicerack.actions[fqdn].failure(
                    'Skipped downtime management interface on Icinga (likely already removed)')

            try:
                remote_host.run_sync('true')
                can_connect = True
            except RemoteExecutionError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Unable to connect to the host, wipe of bootloaders will not be performed**: {e}'
                    .format(e=e))
                can_connect = False

            if can_connect:
                try:
                    # Call wipefs with globbing on all top level devices of type disk reported by lsblk
                    remote_host.run_sync((r"lsblk --all --output 'NAME,TYPE' --paths | "
                                          r"awk '/^\/.* disk$/{ print $1 }' | "
                                          r"xargs -I % bash -c '/sbin/wipefs --all --force %*'"))
                    self.spicerack.actions[fqdn].success('Wiped bootloaders')
                except RemoteExecutionError as e:
                    self.spicerack.actions[fqdn].failure(
                        '**Failed to wipe bootloaders, manual intervention required to make '
                        'it unbootable**: {e}'.format(e=e))

            try:
                ipmi.command(mgmt, ['chassis', 'power', 'off'])
                self.spicerack.actions[fqdn].success('Powered off')
            except IpmiError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Failed to power off, manual intervention required**: {e}'
                    .format(e=e))

            switches = update_netbox(netbox, netbox_data, self.spicerack.dry_run)
            self.spicerack.actions[fqdn].success(
                'Set Netbox status to Decommissioning and deleted all non-mgmt interfaces '
                'and related IPs')

        logger.info('Sleeping for 20s to avoid race conditions...')
        time.sleep(20)

        debmonitor.host_delete(fqdn)
        self.spicerack.actions[fqdn].success('Removed from DebMonitor')

        puppet_master.delete(fqdn)
        self.spicerack.actions[fqdn].success('Removed from Puppet master and PuppetDB')

        if is_virtual:
            logger.info('Issuing Ganeti remove command, it can take up to 15 minutes...')
            try:
                virtual_machine.remove()
                self.spicerack.actions[fqdn].success('VM removed')
            except RemoteExecutionError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Failed to remove VM, manually run gnt-instance remove on the Ganeti '
                    'master for the {cluster} cluster**: {e}'
                    .format(cluster=virtual_machine.cluster, e=e))

            self.sync_ganeti(fqdn, virtual_machine)

        return switches

    def sync_ganeti(self, fqdn, virtual_machine):
        """Force a run of the Ganeti-Netbox sync systemd timer."""
        try:
            # TODO: avoid race conditions to run it at the same time that the systemd timer will trigger it
            self.spicerack.netbox_master_host.run_sync(
                'systemctl start netbox_ganeti_{cluster}_sync.service'
                .format(cluster=virtual_machine.cluster.split('.')[2]))
            # TODO: add polling and validation that it completed to run
            self.spicerack.actions[fqdn].success(
                'Started forced sync of VMs in Ganeti cluster {cluster} to Netbox'
                .format(cluster=virtual_machine.cluster))
        except (DnsError, RemoteExecutionError) as e:
            self.spicerack.actions[fqdn].failure(
                '**Failed to force sync of VMs in Ganeti cluster {cluster} to Netbox**: {e}'
                .format(cluster=virtual_machine.cluster, e=e))

    def run(self):  # pylint: disable=too-many-locals
        """Required by Spicerack API."""
        has_failures = False
        # Check for references in the Puppet and mediawiki-config repositories.
        # TODO: once all the host DNS records are automatically generated from Netbox check also the DNS repository.
        check_patterns_in_repo((
            (self.puppet_master, PUPPET_REPO_PATH),
            (self.puppet_master, PUPPET_PRIVATE_REPO_PATH),
            (self.deployment_host, MEDIAWIKI_CONFIG_REPO_PATH),
        ), self.patterns)

        find_kerberos_credentials(self.kerberos_kadmin, self.decom_hosts)
        phabricator = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        switches = set()
        for fqdn in self.decom_hosts:  # Doing one host at a time to track executed actions.
            try:
                switches.update(self._decommission_host(fqdn))
            except Exception as e:  # pylint: disable=broad-except
                message = 'Host steps raised exception'
                logger.exception(message)
                self.spicerack.actions[fqdn].failure(
                    '**{message}**: {e}'.format(message=message, e=e))

            if self.spicerack.actions[fqdn].has_failures:
                has_failures = True

        logger.info('Sleeping for 3 minutes to get Netbox caches in sync')
        time.sleep(180)
        dns_netbox_args = dns_netbox_argparse().parse_args(
            ['{hosts} decommissioned, removing all IPs except the asset tag one'
             .format(hosts=self.decom_hosts)])
        try:
            dns_netbox_run(dns_netbox_args, self.spicerack)
        except RemoteExecutionError as e:
            message = 'Failed to run the sre.dns.netbox cookbook'
            logger.exception(message)
            self.spicerack.actions[COMMON_STEPS_KEY].failure(
                '**{message}**: {e}'.format(message=message, e=e))
            has_failures = True

        # Run homer once per needed ToR switch
        for switch in switches:
            logger.info('Running Homer on %s, it takes time ⏳ don\'t worry', switch)
            try:
                if not self.spicerack.dry_run:
                    subprocess.run(['/usr/local/bin/homer',
                                    switch, 'commit', str(self.reason)], check=True)
            except subprocess.SubprocessError as e:
                message = 'Failed to run Homer on {switch}'.format(switch=switch)
                logger.exception(message)
                self.spicerack.actions[COMMON_STEPS_KEY].failure(
                    '**{message}**: {e}'.format(message=message, e=e))
                has_failures = True

        suffix = ''
        if has_failures:
            suffix = '**ERROR**: some step on some host failed, check the bolded items above'
            logger.error('ERROR: some step failed, check the task updates.')

        message = '{name} executed by {owner} for hosts: `{hosts}`\n{actions}\n{suffix}'.format(
            name=__name__, owner=self.reason.owner, hosts=self.decom_hosts,
            actions=self.spicerack.actions, suffix=suffix)
        phabricator.task_comment(self.task_id, message)

        return int(has_failures)
