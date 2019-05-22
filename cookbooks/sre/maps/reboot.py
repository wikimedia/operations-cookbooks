"""Maps reboot cookbook

Usage example:
    cookbook sre.maps.reboot --query maps100* --reason 'kernel upgrades'

"""

import argparse
import logging

from datetime import datetime, timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError


__title__ = 'Maps reboot cookbook'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--query', required=True, help='query for selecting maps nodes.')
    parser.add_argument('--task-id', help='task id for the change')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=1, help='Hours of downtime')
    parser.add_argument('--depool', action='store_true', help='Should be depooled')

    return parser


def fail_for_replicate_osm_process(remote_hosts):
    """Raise error if replicate-osm is running in any of the hosts"""
    remote_hosts.run_sync('! pgrep replicate-osm', is_safe=True)


@retry(tries=20, delay=timedelta(seconds=3), backoff_mode='constant', exceptions=(RemoteExecutionError,))
def wait_for_cassandra(remote_host):
    """Wait until cassandra join its cluster.

    Use check_tcp nagios plugin as cassandra is only ready when it has joined its cluster

    """
    remote_host.run_sync('/usr/lib/nagios/plugins/check_tcp -H `hostname` -p 9042', is_safe=True)


def run(args, spicerack):
    """Required by Spicerack API."""
    # Only maps hosts should be selected
    remote_hosts = spicerack.remote().query(args.query)
    all_maps = spicerack.remote().query("A:maps-all".format(query=args.query))
    if remote_hosts.hosts not in all_maps.hosts:
        raise ValueError("All hosts from query: {query} must be member of A:maps-all")

    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
    icinga = spicerack.icinga()

    fail_for_replicate_osm_process(remote_hosts)

    for remote_host in remote_hosts.split(len(remote_hosts)):
        with icinga.hosts_downtimed(remote_host.hosts, reason, duration=timedelta(hours=args.downtime)):
            if args.depool:
                logger.info("Depool %s and wait for current requests to terminate", remote_host)
                remote_host.run_sync('depool', 'sleep 180')

            fail_for_replicate_osm_process(remote_hosts)

            reboot_time = datetime.utcnow()
            logger.info("Rebooting and waiting for %s up", remote_host)
            remote_host.reboot()
            remote_host.wait_reboot_since(reboot_time)

            logger.info("Waiting for cassandra in %s to rejoin its cluster", remote_host)
            wait_for_cassandra(remote_host)
            if args.depool:
                remote_host.run_sync('pool')

            logger.info("Operation completed for %s", remote_host)
