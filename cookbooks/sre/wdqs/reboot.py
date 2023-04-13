"""WDQS reboot cookbook

Usage example:
    cookbook sre.wdqs.reboot --query wdqs100* --reason upgrades --task-id T12345

"""

import argparse
import logging

from datetime import datetime, timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from . import check_hosts_are_valid


__title__ = "WDQS reboot cookbook"
logger = logging.getLogger(__name__)

SERVICES = {
    'wdqs': ['wdqs-updater', 'wdqs-blazegraph', 'wdqs-categories'],
    'wcqs': ['wcqs-updater', 'wcqs-blazegraph'],
}


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--downtime', type=int, default=1, help="Hours of downtime")
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--no-depool', dest='depool', action='store_false', help='Don\'t pool/depool hosts')

    return parser


@retry(tries=20, delay=timedelta(seconds=3), backoff_mode='constant', exceptions=(RemoteExecutionError,))
def wait_for_blazegraph(remote_host):
    """Wait for blazegraph services"""
    remote_host.run_sync('curl http://localhost/readiness-probe > /dev/null')


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_hosts = remote.query(args.query)
    host_kind = check_hosts_are_valid(remote_hosts, remote)
    services = SERVICES[host_kind]

    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    for remote_host in remote_hosts.split(len(remote_hosts)):

        with spicerack.alerting_hosts(remote_host.hosts).downtimed(reason, duration=timedelta(hours=args.downtime)):
            if args.depool:
                logger.info('Depool flag enabled => depooling host before reboot')
                remote_host.run_sync('depool', 'sleep 120')

            # explicit shutdown of Blazegraph instance, to ensure they are not killed by systemd if taking too long
            remote_host.run_sync(*('systemctl stop ' + service for service in services))

            reboot_time = datetime.utcnow()
            remote_host.reboot()
            remote_host.wait_reboot_since(reboot_time)

            logger.info("Forcing puppet run after reboot:\n")
            spicerack.puppet(remote_host).run()

            if args.depool:
                wait_for_blazegraph(remote_host)
                remote_host.run_sync('pool')
                logger.info('Depool flag enabled => pooled host following reboot')
