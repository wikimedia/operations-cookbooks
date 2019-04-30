"""WDQS services restart

Usage example:
    cookbook sre.wdqs.restart-wdqs --query wdqs100* --reason upgrades --task-id T12345

"""

import argparse
import logging

from datetime import timedelta


__title__ = "WDQS services restart cookbook"
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('--task-id', help='task id for the change')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=1, help="Hour(s) of downtime")
    parser.add_argument('--depool', action='store_true', help='This cluster does not use LVS.')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    if 'wdqs' not in args.query:
        raise ValueError("Query ({query}) should only select wdqs host(s)".format(query=args.query))

    remote_hosts = spicerack.remote().query(args.query)
    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_hosts)
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    with icinga.hosts_downtimed(remote_hosts.hosts, reason, duration=timedelta(hours=args.downtime)):
        with puppet.disabled(reason):
            base_commands = [
                'systemctl stop wdqs-updater',
                'systemctl restart wdqs-blazegraph wdqs-categories',
                'sleep 20',
                'systemctl start wdqs-updater'
            ]
            if args.depool:
                commands = ['depool', 'sleep 180', *base_commands, 'pool']
            else:
                commands = base_commands

            remote_hosts.run_async(*commands, batch_size=1)
