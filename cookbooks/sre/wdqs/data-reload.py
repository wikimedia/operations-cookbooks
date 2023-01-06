"""WDQS data reload

Usage example:
    (fresh wikidata reload)
    cookbook sre.wdqs.data-reload --reload-data wikidata --reason "bring new hosts into rotation" \
    --task-id T301167 wdqs1004.eqiad.wmnet

    (wikidata reload after previous failure *after* munging)
    cookbook sre.wdqs.data-reload --reload-data wikidata --reason "bring new hosts into rotation" \
    --task-id T301167 --reuse-munge wdqs1004.eqiad.wmnet
"""

import argparse
import logging

from contextlib import contextmanager
from datetime import datetime, timedelta
import dateutil.parser

from spicerack.kafka import ConsumerDefinition

from cookbooks.sre.wdqs import check_hosts_are_valid, wait_for_updater, get_site, MUTATION_TOPICS, get_hostname

__title__ = "WDQS data reload cookbook"
logger = logging.getLogger(__name__)

NFS_DUMPS = {
    'wikidata': {
        'read_path': '/mnt/nfs/dumps-clouddumps1001.wikimedia.org/'
                     'wikidatawiki/entities/20230102/wikidata-20230102-all-BETA.ttl.bz2',
        'munge_path': '/srv/wdqs/munged',
    },
    'lexeme': {
        'read_path': '/mnt/nfs/dumps-clouddumps1001.wikimedia.org/'
                     'wikidatawiki/entities/20230106/wikidata-20230106-lexemes-BETA.ttl.bz2',
        'munge_path': '/srv/wdqs/lex-munged',
    },
    'commons': {
        'read_path': '/mnt/nfs/dumps-clouddumps1001.wikimedia.org/commonswiki/entities',
        'munge_path': '/srv/query_service/munged',
        'munge_jar_args': ' --wikibaseHost commons.wikimedia.org'
                          ' --conceptUri http://www.wikidata.org'
                          ' --commonsUri https://commons.wikimedia.org',
    }
}

KAFKA_TIMESTAMP_FILEPATH = '/srv/wdqs/last_observed.timestamp'


class StopWatch:
    """Stop watch to measure time."""

    def __init__(self) -> None:
        """Create a new StopWatch initialized with current time."""
        self._start_time = datetime.now()

    def elapsed(self) -> timedelta:
        """Returns the time elapsed since the StopWatch was started."""
        end_time = datetime.now()
        return end_time - self._start_time

    def reset(self):
        """Reset the StopWatch to current time."""
        self._start_time = datetime.now()


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('host', help='select a single WDQS host.')
    parser.add_argument('--task-id', help='task id for the change')
    parser.add_argument('--proxy-server', help='Specify proxy server to use')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=336, help='Hour(s) of downtime')
    parser.add_argument('--depool', action='store_true', help='Should be depooled.')
    parser.add_argument('--reload-data', required=True, choices=['wikidata', 'categories', 'commons'],
                        help='Type of data to reload')
    parser.add_argument('--reuse-munge', action='store_true',
                        help='Reuse munge (use if previous reload failed after munging completed).'
                        'WARNING: Does not perform sanity checks.')

    return parser


def to_ms_timestamp(time_str: str) -> int:
    """Converts string representation of datetime into time since unix epoch in ms"""
    if time_str.isdigit():
        # Input is already a ms timestamp
        return int(time_str)
    dt = dateutil.parser.parse(time_str)
    return int(dt.timestamp() * 1000)


def extract_kafka_timestamp(remote_host, journal_type):
    """Given a remote_host and journal type, parse and return the correct kafka timestamp."""
    dump_path = NFS_DUMPS[journal_type]['read_path']
    cmd = "bzcat {} | head -50 | grep '^wikibase:Dump' -A 5 | grep 'schema:dateModified'".format(dump_path)

    status = next(remote_host.run_sync(cmd))
    timestamp = str(list(status[1].lines())).split('"')[1]
    logger.info('[extract_kafka_timestamp] found %s', timestamp)
    # TODO later we validate the timestamp here (getting patch working first)
    return timestamp


def munge(dumps, remote_host):
    """Run munger for main database and lexeme"""
    logger.info('Running munger for main database and then lexeme')
    stop_watch = StopWatch()
    for dump in dumps:
        logger.info('munging %s', dump['munge_path'])
        stop_watch.reset()
        remote_host.run_sync(
            "rm -rf {munge_path} && mkdir -p {munge_path} && bzcat {path} | "
            "/srv/deployment/wdqs/wdqs/munge.sh -f - -d {munge_path} -- --skolemize {munge_jar_args}"
            .format(path=dump['read_path'],
                    munge_path=dump['munge_path'],
                    munge_jar_args=dump.get('munge_jar_args', ''))
        )
        logger.info('munging %s completed in %s', dump['munge_path'], stop_watch.elapsed())


def reload_commons(remote_host, puppet, kafka, timestamp, consumer_definition, reason):
    """Execute commands on host to reload commons data."""
    logger.info('Preparing to load commons data for blazegraph')
    with puppet.disabled(reason):
        remote_host.run_sync(
            'rm -fv /srv/query_service/data_loaded',
            'systemctl stop wcqs-updater',
            'systemctl stop wcqs-blazegraph',
            'rm -fv /srv/query_service/wcqs.jnl',
            'systemctl start wcqs-blazegraph',
        )

    logger.info('Loading commons dump')
    watch = StopWatch()
    remote_host.run_sync(
        'sleep 60',
        'test -f /srv/query_service/wcqs.jnl',
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wcq -d {munge_path}".format(
            munge_path=NFS_DUMPS['commons']['munge_path']
        )
    )
    logger.info('Commons dump loaded in %s', watch.elapsed())
    kafka.set_consumer_position_by_timestamp(consumer_definition, timestamp)
    logger.info('Set kafka consumer position to %s', timestamp)
    remote_host.run_sync(
        'touch /srv/query_service/data_loaded',
        'systemctl start wcqs-updater'
    )


def reload_wikidata(remote_host, puppet, kafka, timestamp, consumer_definition, reason):
    """Execute commands on host to reload wikidata data."""
    logger.info('Preparing to load wikidata data for blazegraph')
    with puppet.disabled(reason):
        remote_host.run_sync(
            'rm -fv /srv/wdqs/data_loaded',
            'systemctl stop wdqs-updater',
            'systemctl stop wdqs-blazegraph',
            'rm -fv /srv/wdqs/wikidata.jnl',
            'systemctl start wdqs-blazegraph',
        )
    logger.info('Loading wikidata dump')
    watch = StopWatch()
    remote_host.run_sync(
        'sleep 60',
        'test -f /srv/wdqs/wikidata.jnl',
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wdq -d {munge_path}".format(
            munge_path=NFS_DUMPS['wikidata']['munge_path']
        )
    )
    logger.info('Wikidata dump loaded in %s', watch.elapsed())
    logger.info('Loading lexeme dump')
    watch.reset()
    remote_host.run_sync(
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wdq -d {munge_path}".format(
            munge_path=NFS_DUMPS['lexeme']['munge_path']
        )
    )
    logger.info('Lexeme dump loaded in %s', watch.elapsed())
    logger.info('Performing final steps')
    kafka.set_consumer_position_by_timestamp(consumer_definition, timestamp)
    logger.info('Set kafka consumer position to %s', timestamp)
    remote_host.run_sync(
        'touch /srv/wdqs/data_loaded',
        'systemctl start wdqs-updater'
    )


def reload_categories(remote_host, puppet, reason):
    """Execute commands on host to reload categories data."""
    logger.info('Preparing to load data for categories')
    with puppet.disabled(reason):
        remote_host.run_sync(
            'systemctl stop wdqs-categories',
            'rm -fv /srv/wdqs/categories.jnl',
            'systemctl start wdqs-categories'
        )

    logger.info('Loading data for categories')
    watch = StopWatch()
    remote_host.run_sync(
        'sleep 30',
        'test -f /srv/wdqs/categories.jnl',
        '/usr/local/bin/reloadCategories.sh wdqs'
    )
    logger.info('Categories loaded in %s', watch.elapsed())


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_host = remote.query(args.host)
    check_hosts_are_valid(remote_host, remote)

    if len(remote_host) != 1:
        raise ValueError("Only one host is needed. Not {total}({source})".
                         format(total=len(remote_host), source=remote_host))

    alerting_hosts = spicerack.alerting_hosts(remote_host.hosts)
    puppet = spicerack.puppet(remote_host)
    confctl = spicerack.confctl('node')
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    # Get and validate kafka timestamp if not reusing munge
    kafka_timestamp = ''
    if args.reuse_munge:
        results = next(remote_host.run_sync(
                       'cat {}'.format(KAFKA_TIMESTAMP_FILEPATH)))
        kafka_timestamp = results[1].message()
        logger.info('Reusing previously written timestamp of %s', kafka_timestamp)
    else:
        kafka_timestamp = extract_kafka_timestamp(remote_host, args.reload_data)
        # Write observed timestamp to file
        remote_host.run_sync('echo {} > {}'.format(kafka_timestamp, KAFKA_TIMESTAMP_FILEPATH))
        logger.info('Wrote timestamp of %s to %s', kafka_timestamp, KAFKA_TIMESTAMP_FILEPATH)

    if args.reload_data in ['wikidata', 'commons'] and kafka_timestamp is None:
        raise ValueError("We don't have a timestamp, automated timestamp extraction must have failed")

    if 'wikidata' == args.reload_data and not args.reuse_munge:
        dumps = [NFS_DUMPS['wikidata'], NFS_DUMPS['lexeme']]
        munge(dumps, remote_host)

    if 'commons' == args.reload_data and not args.reuse_munge:
        dumps = [NFS_DUMPS['commons']]
        munge(dumps, remote_host)

    @contextmanager
    def noop_change_and_revert():
        yield

    def change_and_revert():
        return confctl.change_and_revert('pooled', True, False, name=remote_host.hosts[0])

    if args.depool:
        depool_host = change_and_revert
    else:
        depool_host = noop_change_and_revert

    def reload_wikibase(reload_fn, mutation_topic):
        prometheus = spicerack.prometheus()
        hostname = get_hostname(args.host)
        consumer_definition = ConsumerDefinition(get_site(hostname, spicerack), 'main', hostname)
        reload_fn(remote_host, puppet, spicerack.kafka(), {mutation_topic: kafka_timestamp},
                  consumer_definition, reason)
        logger.info('Data reload for blazegraph is complete. Waiting for updater to catch up')
        watch = StopWatch()
        wait_for_updater(prometheus, args.site, remote_host)
        logger.info('Caught up on updates in %s', watch.elapsed())

    with alerting_hosts.downtimed(reason, duration=timedelta(hours=args.downtime)):
        with depool_host():
            remote_host.run_sync('sleep 180')
            if 'categories' == args.reload_data:
                reload_categories(remote_host, puppet, reason)
            elif 'wikidata' == args.reload_data:
                reload_wikibase(reload_wikidata, MUTATION_TOPICS['wikidata'])
            elif 'commons' == args.reload_data:
                reload_wikibase(reload_commons, MUTATION_TOPICS['commons'])
