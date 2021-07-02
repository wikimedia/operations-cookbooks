"""Perform a rolling restart of a Cassandra cluster"""
import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('cluster', nargs='?',
                       choices=['aqs', 'restbase-eqiad', 'restbase-dev', 'sessionstore',
                                'restbase-codfw'],
                       help=('The name of the Cassandra cluster to work on. This refers to '
                             'a Cumin alias. Alternatively you can pass an alternative Cumin '
                             'host query using the --query argument'))
    group.add_argument('--query', help='A cumin query string')
    parser.add_argument('-r', '--reason', help='The reason for performing the restart',
                        required=True)
    parser.add_argument('--batch-sleep-seconds', type=float, default=300.0,
                        help="Seconds to sleep between each host.")
    parser.add_argument('--instance-sleep-seconds', type=int, default=10,
                        help="Seconds to sleep between each Cassandra instance restart.")
    return parser


def run(args, spicerack):
    """Restart all Cassandra nodes on a given cluster"""
    if args.cluster is not None:
        query = 'A:{}'.format(args.cluster)
    else:
        query = args.query
    ensure_shell_is_durable()

    cassandra_nodes = spicerack.remote().query(query)
    icinga_hosts = spicerack.icinga_hosts(cassandra_nodes.hosts)
    reason = spicerack.admin_reason(args.reason)

    logger.info('Checking that all Cassandra nodes are reported up by their systemd unit status.')
    # perhaps we should create a c-foreach-status script?
    status_cmd = """\
            STRING=''; \
            for i in $(c-ls) ; do STRING="${STRING} cassandra-${i}" ; done ; \
            systemctl status $STRING\
            """
    cassandra_nodes.run_sync(status_cmd)

    with icinga_hosts.downtimed(reason, duration=timedelta(minutes=240)):
        cassandra_nodes.run_sync(
            'c-foreach-restart -d ' + str(args.instance_sleep_seconds) + ' -a 20 -r 12',
            batch_size=1,
            batch_sleep=args.batch_sleep_seconds)

    logger.info('All Cassandra restarts completed!')
