"""Postgresql streaming replication initialization

Usage example:
    cookbook sre.postgresql.postgres-init --replica maps1003.eqiad.wmnet --reason "stretch migration"

"""
import argparse
import logging

from datetime import timedelta

__title__ = "Postgres replica initialization cookbook"
logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for sre.postgresql.postgres-init cookbooks."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--replica', required=True, help='FQDN of replica node.')
    parser.add_argument('--reason', required=True, help='Admin reason')
    parser.add_argument('--pgversion', default='',
                        help='Postgresql version default: autodetect')
    parser.add_argument('--downtime', type=int, default=6,
                        help='Hours of downtime default: %(default)s')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--depool', action='store_true', help='Should be depooled')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    # Make sure only a single postgresql replica is selected
    replica = spicerack.remote().query("{replica} and C:postgresql::slave".format(replica=args.replica))
    if len(replica) != 1:
        raise ValueError("Please select one node at a time. Querying for '{replica}' returns {total} node(s)".format(
            replica=args.replica, total=len(replica)
        ))
    alerting_hosts = spicerack.alerting_hosts(replica.hosts)
    puppet = spicerack.puppet(replica)
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    with alerting_hosts.downtimed(reason, duration=timedelta(hours=args.downtime)):
        with puppet.disabled(reason):
            if args.depool:
                replica.run_sync('depool', 'sleep 180')
            replica.run_sync("/usr/local/bin/pg-resync-replica {}".format(args.pgversion))
        puppet.run()
        if args.depool:
            replica.run_sync('pool')
