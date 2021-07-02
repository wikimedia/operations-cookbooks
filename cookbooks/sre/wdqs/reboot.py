"""WDQS reboot cookbook

Usage example:
    cookbook sre.wdqs.reboot --query wdqs100* --reason upgrades --task-id T12345

"""

import argparse
import logging

from datetime import datetime, timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from . import check_host_is_wdqs


__title__ = "WDQS reboot cookbook"
logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--downtime', type=int, default=1, help="Hours of downtime")
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--depool', action='store_true', help='Should be depooled')

    return parser


@retry(tries=20, delay=timedelta(seconds=3), backoff_mode='constant', exceptions=(RemoteExecutionError,))
def wait_for_blazegraph(remote_host):
    """Wait for blazegraph services"""
    remote_host.run_sync('curl http://localhost/readiness-probe > /dev/null')


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_hosts = remote.query(args.query)
    check_host_is_wdqs(remote_hosts, remote)

    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    for host in remote_hosts.hosts:
        remote_host = remote.query(host)

        with spicerack.icinga_hosts(remote_host.hosts).downtimed(reason, duration=timedelta(hours=args.downtime)):
            if args.depool:
                remote_host.run_sync('depool', 'sleep 180')

            # explicit shutdown of Blazegraph instance, to ensure they are not killed by systemd if taking too long
            remote_host.run_sync(
                'systemctl stop wdqs-blazegraph',
                'systemctl stop wdqs-categories',
                'systemctl stop wdqs-updater')

            reboot_time = datetime.utcnow()
            remote_host.reboot()
            remote_host.wait_reboot_since(reboot_time)

            if args.depool:
                wait_for_blazegraph(remote_host)
                remote_host.run_sync('pool')
