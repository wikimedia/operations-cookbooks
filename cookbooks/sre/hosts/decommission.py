"""Decommission a host from all inventories."""
import logging
import time


from pynetbox.core.query import RequestError
from wmflib.dns import DnsError, DnsNotFound
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.alertmanager import AlertmanagerError
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.icinga import IcingaError
from spicerack.netbox import MANAGEMENT_IFACE_NAME, NetboxError
from spicerack.ipmi import IpmiError
from spicerack.remote import NodeSet, RemoteError, RemoteExecutionError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import (
    check_patterns_in_repo,
    find_kerberos_credentials,
    get_grep_patterns,
    GitRepoPath,
    DEPLOYMENT_HOST,
    MEDIAWIKI_CONFIG_REPO_PATH,
    KERBEROS_KADMIN_CUMIN_ALIAS,
    PUPPETSERVER_REPO_PATH,
    PUPPETSERVER_PRIVATE_REPO_PATH,
    COMMON_STEPS_KEY,
    DEPLOYMENT_CHARTS_REPO_PATH,
    AUTHDNS_REPO_PATH,
)
from cookbooks.sre.network import configure_switch_interfaces


logger = logging.getLogger(__name__)


# Temporary workaround as Netbox sometimes fails with 500 when updating the device because of a stale reference
# to one of the primary IPs.
@retry(tries=4, exceptions=(RequestError,))
def update_netbox(netbox, netbox_data, keep_mgmt_dns, dry_run):
    """Delete all non-mgmt IPs, disable remote interfaces/vlan and set the status to Decommissioning."""
    # TODO: this is needed instead of calling put_hosts_status() because Netbox cache doesn't get invalidated
    #       immediately making the call to fail. A sleep of 10 seconds did not fix the issue either.
    device = netbox.api.dcim.devices.get(netbox_data['id'])
    device.primary_ip4_id = None
    device.primary_ip6_id = None
    device.status = 'decommissioning'
    if not dry_run:
        device.save()

    for interface in netbox.api.dcim.interfaces.filter(device_id=netbox_data['id']):
        if interface.mgmt_only:  # Leave it but remove the DNS name
            if keep_mgmt_dns:
                logger.info('Skipping removal of DNS names on interface %s', interface.name)
                continue

            if interface.count_ipaddresses > 0:
                for ip in netbox.api.ipam.ip_addresses.filter(interface_id=interface.id):
                    logger.info('Unset DNS name for IP %s on %s', ip.address, ip.assigned_object.name)
                    if not dry_run:
                        ip.dns_name = ''
                        ip.save()

            continue

        # If the interface is connected to another interface (and not a circuit, etc)
        if interface.connected_endpoints and interface.connected_endpoints_type == 'dcim.interface':
            remote_interface = netbox.api.dcim.interfaces.get(interface.connected_endpoints[0].id)
            if remote_interface.device.role.slug not in ('asw', 'cloudsw'):
                logger.debug('Skipping interface %s, is connected to %s (%s), that is not a switch',
                             interface.name, remote_interface.device.name, remote_interface.device.role.name)
                continue

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


class DecommissionHost(CookbookBase):
    """Decommission a host from all inventories.

    It works for both Physical and Virtual hosts.
    If the query doesn't match any hosts allow to proceed with hostname expansion.

    List of actions performed on each host:
    - Check if any reference was left in the Puppet (both public and private) or
      mediawiki-config repositories and ask for confirmation before proceeding
      if there is any match.
    - Downtime the host on Icinga/Alertmanager (it will be removed at the next Puppet run on
      the Icinga host).
    - Detect if Physical or Virtual host based on Netbox data.
    - If virtual host (Ganeti VM)
      - Ganeti shutdown (tries OS shutdown first, pulls the plug after 2 minutes)
      - Force Ganeti->Netbox sync of VMs to update its state and avoid
        Netbox Report errors
    - If physical host
      - Downtime the management host on Icinga/Alertmanager (it will be removed at the next
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
        parser = super().argument_parser()
        parser.add_argument('query', help=('Cumin query to match the host(s) to act upon. At most 5 at a time, with '
                                           '--force at most 20 at a time.'))
        parser.add_argument('-t', '--task-id', required=True, help='the Phabricator task ID (e.g. T12345)')
        parser.add_argument('--force', action='store_true',
                            help='Bypass the default limit of 5 hosts at a time, but only up to 20 hosts.')
        parser.add_argument('--keep-mgmt-dns', action='store_true',
                            help='Do not remove DNS names from management addresses')

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
                'ATTENTION: the query DOES NOT MATCH any host in PuppetDB\n'
                f'Hostname expansion matches {len(decom_hosts)} UNVERIFIED hosts: {decom_hosts}\n'
                'Do you want to proceed anyway?'
            )
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

        self.dns = spicerack.dns()
        netbox = spicerack.netbox()
        self.netbox_servers = {}
        self.ipmi_hosts = {}
        for fqdn in self.decom_hosts:
            hostname = fqdn.split('.')[0]
            self.netbox_servers[hostname] = spicerack.netbox_server(hostname)
            try:
                netbox_fqdn = self.netbox_servers[hostname].fqdn
                if netbox_fqdn and fqdn != netbox_fqdn:
                    raise RuntimeError(f'Mismatched FQDN {fqdn} does not match Netbox FQDN {netbox_fqdn}')
            except NetboxError:
                spicerack.actions[fqdn].warning(
                    f'//Missing DNSName in Nebox for {hostname}, unable to verify it.//')

            try:
                self.dns.resolve_ips(fqdn)
            except DnsNotFound:
                spicerack.actions[fqdn].warning(
                    f'//Missing DNS record for {fqdn}, the steps requiring DNS will fail.//')

            if self.netbox_servers[hostname].virtual:
                continue

            # Only for physical hosts
            try:
                mgmt_target = self.netbox_servers[hostname].mgmt_fqdn
                self.dns.resolve_ips(mgmt_target)
            except (DnsNotFound, NetboxError):
                mgmt_target = netbox.api.ipam.ip_addresses.get(
                    interface=MANAGEMENT_IFACE_NAME, device=hostname).address.split('/')[0]
                spicerack.actions[fqdn].warning(
                    f'//Unable to find/resolve the mgmt DNS record, using the IP instead: {mgmt_target}//')

            self.ipmi_hosts[hostname] = spicerack.ipmi(mgmt_target)
            try:
                self.ipmi_hosts[hostname].check_connection()
            except IpmiError:
                ask_confirmation(f'WARNING: remote IPMI connection test failed for host {hostname}. The host will not '
                                 'be shutdown. You can either continue (go) as is or try to fix the problem first '
                                 '(abort). See https://wikitech.wikimedia.org/wiki/Ipmi for troubleshooting.')

        ask_confirmation(
            'ATTENTION: destructive action for {n} hosts: {hosts}\nAre you sure to proceed?'
            .format(n=len(self.decom_hosts), hosts=self.decom_hosts))

        self.spicerack = spicerack
        self.task_id = args.task_id
        self.keep_mgmt_dns = args.keep_mgmt_dns
        self.puppet_server = spicerack.puppet_server().server_host
        self.kerberos_kadmin = self.remote.query(KERBEROS_KADMIN_CUMIN_ALIAS)
        self.deployment_host = self.remote.query(self.dns.resolve_cname(DEPLOYMENT_HOST))
        self.patterns = get_grep_patterns(self.dns, self.decom_hosts)
        self.reason = self.spicerack.admin_reason('Host decommission', task_id=self.task_id)
        self.authdns_hosts = spicerack.authdns_active_hosts

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for hosts {}'.format(self.decom_hosts)

    def _decommission_host(self, fqdn):  # noqa: MC0001
        """Perform all the decommissioning actions on a single host and return its switch if physical."""
        hostname = fqdn.split('.')[0]
        puppet_master = self.spicerack.puppet_master()
        puppet_server = self.spicerack.puppet_server()

        debmonitor = self.spicerack.debmonitor()
        netbox = self.spicerack.netbox(read_write=True)
        netbox_server = self.netbox_servers[hostname]
        netbox_data = netbox_server.as_dict()
        ganeti = self.spicerack.ganeti()

        # Using the Direct Cumin backend to support also hosts already removed from PuppetDB
        remote_host = self.remote.query('D{' + fqdn + '}')

        # Downtime on Icinga/Alertmanager both the host and the mgmt host (later below), they will be removed by Puppet
        try:
            self.spicerack.alerting_hosts([fqdn]).downtime(self.reason)
            self.spicerack.actions[fqdn].success('Downtimed host on Icinga/Alertmanager')
        except IcingaError:
            self.spicerack.actions[fqdn].warning(
                '//Host not found on Icinga, unable to downtime it//')
        except RemoteExecutionError:
            self.spicerack.actions[fqdn].warning(
                '//Failed to downtime host on Icinga//')

        if netbox_server.virtual:
            ganeti_cluster = netbox.api.virtualization.virtual_machines.get(name=hostname).cluster.group
            virtual_machine = ganeti.instance(fqdn, cluster=ganeti_cluster)
            self.spicerack.actions[fqdn].success('Found Ganeti VM')

            try:
                virtual_machine.shutdown(timeout=0)
                self.spicerack.actions[fqdn].success('VM shutdown')
            except RemoteExecutionError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Failed to shutdown VM, manually run gnt-instance remove on the Ganeti '
                    'master for the {cluster} cluster**: {e}'
                    .format(cluster=virtual_machine.cluster, e=e))

            self.sync_ganeti(fqdn, virtual_machine)

        else:  # Physical host
            self.spicerack.actions[fqdn].success('Found physical host')
            try:
                self.spicerack.alertmanager_hosts([f'{hostname}.mgmt'], verbatim_hosts=True).downtime(self.reason)
                self.spicerack.actions[fqdn].success(
                    'Downtimed management interface on Alertmanager')
            except AlertmanagerError:
                self.spicerack.actions[fqdn].warning('//Failed to downtime management interface on Alertmanager//')

            try:
                remote_host.run_sync('true')
                can_connect = True
            except RemoteExecutionError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Unable to connect to the host, wipe of swraid, partition-table and '
                    'filesystem signatures will not be performed**: {e}'.format(e=e))
                can_connect = False

            if can_connect:
                try:
                    remote_host.run_sync('swapoff -a')
                    # Call wipefs with globbing on all top level devices of type disk reported by lsblk
                    remote_host.run_sync((r"lsblk --all --output 'NAME,TYPE' --paths | "
                                          r"awk '/^\/.* disk$/{ print $1 }' | "
                                          r"xargs -I % bash -c '/sbin/wipefs --all --force %*'"))
                    self.spicerack.actions[fqdn].success('Wiped all swraid, partition-table and filesystem signatures')
                except RemoteExecutionError as e:
                    self.spicerack.actions[fqdn].failure(
                        '**Failed to wipe swraid, partition-table and filesystem signatures, manual '
                        'intervention required to make it unbootable**: {e}'.format(e=e))

            try:
                if self.ipmi_hosts[hostname].power_status().lower() == 'off':
                    self.spicerack.actions[fqdn].success('Host is already powered off')
                else:
                    self.ipmi_hosts[hostname].command(['chassis', 'power', 'off'])
                    self.spicerack.actions[fqdn].success('Powered off')
            except IpmiError as e:
                self.spicerack.actions[fqdn].failure(
                    '**Failed to power off, manual intervention required**: {e}'
                    .format(e=e))

            update_netbox(netbox, netbox_data, self.keep_mgmt_dns, self.spicerack.dry_run)
            self.spicerack.actions[fqdn].success(
                '[Netbox] Set status to Decommissioning, deleted all non-mgmt IPs,  '
                'updated switch interfaces (disabled, removed vlans, etc)')

            configure_switch_interfaces(self.remote, netbox, netbox_data, self.spicerack.verbose)
            self.spicerack.actions[fqdn].success('Configured the linked switch interface(s)')

        if not self.spicerack.dry_run:
            logger.info('Sleeping for 20s to avoid race conditions...')
            time.sleep(20)

        debmonitor.host_delete(fqdn)
        self.spicerack.actions[fqdn].success('Removed from DebMonitor')

        puppet_master.delete(fqdn)
        puppet_server.delete(fqdn)
        self.spicerack.actions[fqdn].success('Removed from Puppet master and PuppetDB')

        if netbox_server.virtual:
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

    def sync_ganeti(self, fqdn, virtual_machine):
        """Force a run of the Ganeti-Netbox sync systemd timer."""
        try:
            # TODO: avoid race conditions to run it at the same time that the systemd timer will trigger it
            self.spicerack.netbox_master_host.run_sync(
                'systemctl start netbox_ganeti_{cluster}_sync.service'
                .format(cluster=virtual_machine.cluster))
            # TODO: add polling and validation that it completed to run
            self.spicerack.actions[fqdn].success(
                'Started forced sync of VMs in Ganeti cluster {cluster} to Netbox'
                .format(cluster=virtual_machine.cluster))
        except (DnsError, RemoteExecutionError) as e:
            self.spicerack.actions[fqdn].failure(
                '**Failed to force sync of VMs in Ganeti cluster {cluster} to Netbox**: {e}'
                .format(cluster=virtual_machine.cluster, e=e))

    def run(self):
        """Required by Spicerack API."""
        has_failures = False
        # Check for references in the Puppet and mediawiki-config repositories.
        check_patterns_in_repo((
            GitRepoPath(remote_host=self.puppet_server, path=PUPPETSERVER_REPO_PATH, pathspec=':!manifests/site.pp'),
            GitRepoPath(remote_host=self.puppet_server, path=PUPPETSERVER_PRIVATE_REPO_PATH),
            GitRepoPath(remote_host=self.deployment_host, path=MEDIAWIKI_CONFIG_REPO_PATH),
            GitRepoPath(remote_host=self.deployment_host, path=DEPLOYMENT_CHARTS_REPO_PATH),
            GitRepoPath(remote_host=self.authdns_hosts, path=AUTHDNS_REPO_PATH),
        ), self.patterns)

        find_kerberos_credentials(self.kerberos_kadmin, self.decom_hosts)
        phabricator = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

        lock = self.spicerack.lock()
        for fqdn in self.decom_hosts:  # Doing one host at a time to track executed actions.
            with lock.acquired(f'sre.hosts.decommission:{fqdn.split(".")[0]}', concurrency=1, ttl=600):
                try:
                    self._decommission_host(fqdn)
                except Exception as e:  # pylint: disable=broad-except
                    message = 'Host steps raised exception'
                    logger.exception(message)
                    self.spicerack.actions[fqdn].failure(
                        '**{message}**: {e}'.format(message=message, e=e))

            if self.spicerack.actions[fqdn].has_failures:
                has_failures = True

        if not self.spicerack.dry_run:
            logger.info('Sleeping for 3 minutes to get netbox caches in sync')
            time.sleep(180)

        netbox_ret = self.spicerack.run_cookbook(
            'sre.dns.netbox', [f'{self.decom_hosts} decommissioned, removing all IPs except the asset tag one'])
        if netbox_ret:
            message = 'Failed to run the sre.dns.netbox cookbook, run it manually'
            logger.error(message)
            self.spicerack.actions[COMMON_STEPS_KEY].failure(f'**{message}**')
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
