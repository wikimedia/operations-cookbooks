"""Decommission a host from all inventories.

It works for both Physical and Virtual hosts. On VMs some steps are not yet supported

List of actions performed:
- Downtime the host and its management interface on Icinga
  (it will be removed at the next Puppet run on the Icinga host)
- Wipe bootloaders to prevent it from booting again
- Pull the plug (power off without shutdown on physical hosts, shutdown on VMs for now)
- Remove it from DebMonitor
- Remove it from Puppet master and PuppetDB
- Update Netbox state
- Update the related Phabricator task

Usage example:
    cookbook sre.hosts.decommission -t T12345 mw1234.codfw.wmnet

"""
import argparse
import logging

from cumin.transports import Command
from spicerack.interactive import ask_confirmation
from spicerack.ipmi import IpmiError
from spicerack.management import ManagementError
from spicerack.remote import RemoteExecutionError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


__title__ = 'Decommission a host from all inventories.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('query', help=('Cumin query to match the host(s) to act upon. At most 5 at a time, with '
                                       '--force at most 20 at a time.'))
    parser.add_argument('-t', '--task-id', required=True, help='the Phabricator task ID (e.g. T12345)')
    parser.add_argument('--force', help='Bypass the default limit of 5 hosts at a time, but only up to 20 hosts.')

    return parser


def _decommission_host(host, spicerack, reason):  # noqa: MC0001
    """Perform all the decommissioning actions on a single host."""
    failed = False
    actions = []

    icinga = spicerack.icinga()
    remote = spicerack.remote()
    puppet_master = spicerack.puppet_master()
    debmonitor = spicerack.debmonitor()
    management = spicerack.management()
    ipmi = spicerack.ipmi()
    netbox = spicerack.netbox()

    remote_host = remote.query(host)

    # Downtime on Icinga both the host and the mgmt host, they will be removed by Puppet
    # Doing one host at a time to track executed actions.
    try:
        icinga.downtime_hosts([host], reason)
        actions.append('Downtimed host on Icinga')
    except RemoteExecutionError:
        actions.append('Failed downtime host on Icinga (likely already removed)')

    mgmt = None
    try:
        mgmt = management.get_fqdn(host)
    except ManagementError:
        actions.append('No management interface found (likely a VM)')

    if mgmt is not None:
        try:
            icinga.downtime_hosts([mgmt], reason)
            actions.append('Downtimed management interface on Icinga')
        except RemoteExecutionError:
            actions.append('Skipped downtime management interface on Icinga (likely already removed)')

    try:
        remote_host.run_sync('true')
        can_connect = True
    except RemoteExecutionError as e:
        actions.append(
            '**Unable to connect to the host, wipe of bootloaders will not be performed**: {e}'.format(e=e))
        can_connect = False
        failed = True

    if can_connect:
        try:
            # Call wipefs with globbing on all top level devices of type disk reported by lsblk
            remote_host.run_sync((r"lsblk --all --output 'NAME,TYPE' --paths | "
                                  r"awk '/^\/.* disk$/{ print $1 }' | "
                                  r"xargs -I % bash -c '/sbin/wipefs --all --force %*'"))
            actions.append('Wiped bootloaders')
        except RemoteExecutionError as e:
            actions.append(('**Failed to wipe bootloaders, manual intervention required to make it '
                            'unbootable**: {e}').format(e=e))
            failed = True

    if mgmt is not None:  # Physical host
        try:
            ipmi.command(mgmt, ['chassis', 'power', 'off'])
            actions.append('Powered off')
        except IpmiError as e:
            actions.append('**Failed to power off, manual intervention required**: {e}'.format(e=e))
            failed = True

        netbox.put_host_status(host.split('.')[0], 'Decommissioning')
        actions.append('Set Netbox status to Decommissioning')

    else:  # Assuming VM, pull the plug not yet supported, trying normal shutdown
        try:
            remote_host.run_sync(Command('nohup shutdown -h now &> /dev/null & exit', timeout=30))
            actions.append('Shutdown issued. **Verify it manually, verification not yet supported**')
        except RemoteExecutionError as e:
            actions.append(('**Failed to shutdown, manual intervention required**: {e}').format(e=e))
            failed = True

        actions.append('Set Netbox status on VM not yet supported: **manual intervention required**')

    debmonitor.host_delete(host)
    actions.append('Removed from DebMonitor')

    puppet_master.delete(host)
    actions.append('Removed from Puppet master and PuppetDB')

    return failed, actions


def run(args, spicerack):
    """Required by Spicerack API."""
    have_failures = False
    remote = spicerack.remote()
    decom_hosts = remote.query(args.query).hosts
    if len(decom_hosts) > 20:
        logger.error('Matched %d hosts, aborting. (max 20 with --force, 5 without)', len(decom_hosts))
        return 1
    elif len(decom_hosts) > 5:
        if args.force:
            logger.info('Authorized decommisioning of %s hosts with --force', len(decom_hosts))
        else:
            logger.error('Matched %d hosts, and --force not set aborting. (max 20 with --force, 5 without)',
                         len(decom_hosts))
            return 1

    ask_confirmation('ATTENTION: destructive action for {n} hosts: {hosts}\nAre you sure to proceed?'.format(
        n=len(decom_hosts), hosts=decom_hosts))
    reason = spicerack.admin_reason('Host decommission', task_id=args.task_id)
    phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    hosts_actions = []
    for host in decom_hosts:
        try:
            failed, actions = _decommission_host(host, spicerack, reason)
        except Exception as e:
            failed = True
            actions = ['Host steps raised exception: {e}'.format(e=e)]

        success = 'PASS' if not failed else 'FAIL'
        hosts_actions.append('-  {host} (**{success}**)'.format(host=host, success=success))
        hosts_actions += ['  - {action}'.format(action=action) for action in actions]
        if failed:
            have_failures = True

    if have_failures:
        hosts_actions.append('**ERROR**: some step on some host failed, check the bolded items above')
        logger.error('ERROR: some step failed, check the task updates.')

    message = ('{name} executed by {owner} for hosts: `{hosts}`\n{actions}').format(
        name=__name__, owner=reason.owner, hosts=decom_hosts, actions='\n'.join(hosts_actions))
    phabricator.task_comment(args.task_id, message)

    return have_failures
