"""Decommission a host from all inventories.

- Remove it from Puppet master and PuppetDB
- Downtime the host and its management interface on Icinga
  (it will be removed at the next Puppet run on the Icinga host)
- Remove it from DebMonitor
- Update the related Phabricator task

Usage example:
    cookbook sre.hosts.decommission -t T12345 mw1234.codfw.wmnet

"""
import argparse
import logging

from collections import defaultdict

from spicerack.management import ManagementError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


__title__ = 'Decommission a host from all inventories.'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('-t', '--task-id', required=True, help='the Phabricator task ID (e.g. T12345)')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    icinga = spicerack.icinga()
    decom_hosts = spicerack.remote().query(args.query).hosts
    puppet_master = spicerack.puppet_master()
    debmonitor = spicerack.debmonitor()
    reason = spicerack.admin_reason('Host decommission', task_id=args.task_id)
    management = spicerack.management()
    phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    actions = defaultdict(list)

    for host in decom_hosts:
        puppet_master.delete(host)
        actions[host].append('Removed from Puppet master and PuppetDB')

        # Downtime on Icinga both the host and the mgmt host, they will be removed by Puppet
        # Doing one host at a time to track executed actions.
        try:
            icinga.downtime_hosts([host], reason)
            actions[host].append('Downtimed host on Icinga')
        except RuntimeError:
            actions[host].append('Skipped downtime host on Icinga (likely already removed)')

        try:
            mgmt = management.get_fqdn(host)
            try:
                icinga.downtime_hosts([mgmt], reason)
                actions[host].append('Downtimed management interface on Icinga')
            except RuntimeError:
                actions[host].append('Skipped downtime management interface on Icinga (likely already removed)')
        except ManagementError:
            actions[host].append('No management interface found (likely a VM)')

        debmonitor.host_delete(host)
        actions[host].append('Removed from DebMonitor')

    hosts_actions = []
    for host, host_actions in actions.items():
        hosts_actions.append('-  {host}'.format(host=host))
        hosts_actions += ['  - {action}'.format(action=action) for action in host_actions]

    message = ('{name} executed by {owner} for hosts: `{hosts}`\n{actions}').format(
        name=__name__, user=reason.owner, hosts=decom_hosts, actions='\n'.join(hosts_actions))
    phabricator.task_comment(args.phab_task_id, message)
