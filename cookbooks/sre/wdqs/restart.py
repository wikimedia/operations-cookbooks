"""WDQS services restart

Usage example:
    cookbook sre.wdqs.restart --query wdqs100* --reason upgrades --task-id T12345
"""

import argparse
import logging

from datetime import timedelta

from . import check_hosts_are_valid


__title__ = "WDQS services restart cookbook"
logger = logging.getLogger(__name__)


RESTART = {
    'wdqs': [
        'systemctl stop wdqs-updater',
        'systemctl restart wdqs-blazegraph wdqs-categories',
        'sleep 20',
        'systemctl start wdqs-updater'
    ],
    'wcqs': [
        'systemctl stop wcqs-updater',
        'systemctl restart wcqs-blazegraph',
        'sleep 20',
        'systemctl start wcqs-updater'
    ],
}


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('--task-id', help='task id for the change')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=1, help="Hour(s) of downtime")
    parser.add_argument('--no-depool', action='store_true', help='Don\'t depool host (use for non-lvs-managed hosts)')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_hosts = remote.query(args.query)
    host_kind = check_hosts_are_valid(remote_hosts, remote)

    alerting_hosts = spicerack.alerting_hosts(remote_hosts.hosts)
    puppet = spicerack.puppet(remote_hosts)
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    with alerting_hosts.downtimed(reason, duration=timedelta(hours=args.downtime)):
        with puppet.disabled(reason):
            base_commands = RESTART[host_kind]
            if args.no_depool:
                commands = base_commands
            else:
                commands = ['depool', 'sleep 180', *base_commands, 'pool']

            remote_hosts.run_async(*commands, batch_size=1)
