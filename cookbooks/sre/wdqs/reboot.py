"""WDQS reboot cookbook

Usage example:
    cookbook sre.wdqs.reboot --query wdqs100* --reason upgrades --task-id T12345

"""

import argparse
import logging

from datetime import datetime, timedelta, timezone

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from . import check_hosts_are_valid


logger = logging.getLogger(__name__)

# Stopped in order, before the reboot. Only wdqs::main and
# wdqs::internal_main run the categories instance, so it is stopped only
# where its unit exists; a plain `systemctl stop` of a missing unit exits 5
# and would abort the cookbook after the host is already depooled.
SERVICES = {
    'wdqs': ['wdqs-updater', 'wdqs-blazegraph'],
    'wcqs': ['wcqs-updater', 'wcqs-blazegraph'],
}
OPTIONAL_SERVICES = {
    'wdqs': ['wdqs-categories'],
    'wcqs': [],
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


def stop_commands(host_kind):
    """Return the ordered service stop commands for a host kind."""
    commands = ['systemctl stop ' + service for service in SERVICES[host_kind]]
    for service in OPTIONAL_SERVICES[host_kind]:
        commands.append(
            'if systemctl list-unit-files --quiet {unit}.service | grep -q .; '
            'then systemctl stop {unit}; fi'.format(unit=service))
    return commands


@retry(tries=20, delay=timedelta(seconds=3), backoff_mode='constant', exceptions=(RemoteExecutionError,))
def wait_for_blazegraph(remote_host):
    """Wait for blazegraph services"""
    remote_host.run_sync('curl http://localhost/readiness-probe > /dev/null')


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_hosts = remote.query(args.query)
    host_kind = check_hosts_are_valid(remote_hosts, remote)

    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    for remote_host in remote_hosts.split(len(remote_hosts)):

        with spicerack.alerting_hosts(remote_host.hosts).downtimed(reason, duration=timedelta(hours=args.downtime)):
            if args.depool:
                logger.info('Depool flag enabled => depooling host before reboot')
                remote_host.run_sync('depool', 'sleep 120')

            # explicit shutdown of Blazegraph instance, to ensure they are not killed by systemd if taking too long
            remote_host.run_sync(*stop_commands(host_kind))

            reboot_time = datetime.now(timezone.utc)
            remote_host.reboot()
            remote_host.wait_reboot_since(reboot_time)

            logger.info("Forcing puppet run after reboot:\n")
            spicerack.puppet(remote_host).run()

            if args.depool:
                wait_for_blazegraph(remote_host)
                remote_host.run_sync('pool')
                logger.info('Depool flag enabled => pooled host following reboot')
