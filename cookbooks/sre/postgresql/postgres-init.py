"""Postgresql streaming replication initialization

Usage example:
    cookbook sre.postgresql.postgres-init --slave maps1003.eqiad.wmnet --reason "stretch migration"

"""
import argparse
import logging

from datetime import timedelta

__title__ = "Postgres slave initialization cookbook"
logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for sre.postgresql.postgres-init cookbooks."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--slave', required=True, help='FQDN of slave node.')
    parser.add_argument('--reason', required=True, help='Admin reason')
    parser.add_argument('--pgversion', default='9.6',
                        help='Postgresql version default: %(default)s')
    parser.add_argument('--downtime', type=int, default=6,
                        help='Hours of downtime default: %(default)s')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--depool', action='store_true', help='Should be depooled')

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    # Make sure only postgresql slave is selected
    slave = spicerack.remote().query("{slave} and C:postgresql::slave".format(slave=args.slave))
    if len(slave) != 1:
        raise ValueError("Please select one node at a time. Querying for '{slave}' returns {total} node(s)".format(
            slave=args.slave, total=len(slave)
        ))
    icinga = spicerack.icinga()
    puppet = spicerack.puppet(slave)
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    with icinga.hosts_downtimed(slave.hosts, reason, duration=timedelta(hours=args.downtime)):
        with puppet.disabled(reason):
            if args.depool:
                slave.run_sync('depool', 'sleep 180')

            slave.run_sync(
                "systemctl stop postgresql",
                "rm -R /srv/postgresql/{pgversion}/main".format(pgversion=args.pgversion),
                "PGHOST=`cut -d':' -f1 /etc/postgresql/{pgversion}/main/.pgpass` "
                "PGPASSFILE=/etc/postgresql/{pgversion}/main/.pgpass sudo -E -u postgres "
                "/usr/bin/pg_basebackup -X stream -D /srv/postgresql/{pgversion}/main -U replication -w".format(
                    pgversion=args.pgversion),
            )
        puppet.run()
        if args.depool:
            slave.run_sync('pool')
