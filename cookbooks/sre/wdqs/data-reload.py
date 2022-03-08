"""WDQS data reload

Usage example:
    cookbook sre.wdqs.data-reload wdqs1010.eqiad.wmnet --reason 'fix issues' --task-id T12345

"""

import argparse
import logging
import os

from contextlib import contextmanager
from datetime import datetime, timedelta

from spicerack.kafka import ConsumerDefinition
from spicerack.remote import RemoteExecutionError

from cookbooks.sre.wdqs import check_hosts_are_valid, wait_for_updater, get_site, MUTATION_TOPICS, get_hostname

__title__ = "WDQS data reload cookbook"
logger = logging.getLogger(__name__)

DUMPS = {
    'wikidata': {
        'url': 'https://dumps.wikimedia.your.org/wikidatawiki/entities/latest-all.ttl.bz2',
        'munge_path': '/srv/wdqs/munged',
        'path': '/srv/wdqs/latest-all.ttl.bz2',
    },
    'lexeme': {
        'url': 'https://dumps.wikimedia.your.org/wikidatawiki/entities/latest-lexemes.ttl.bz2',
        'munge_path': '/srv/wdqs/lex-munged',
        'path': '/srv/wdqs/latest-lexemes.ttl.bz2',
    },
    'commons': {
        'url': 'https://dumps.wikimedia.your.org/commonswiki/entities/latest-mediainfo.ttl.bz2',
        'munge_path': '/srv/query_service/munged',
        'munge_jar_args': '--wikibaseHost commons.wikimedia.org'
                          ' --conceptUri http://www.wikidata.org'
                          ' --commonsUri https://commons.wikimedia.org',
        'path': '/srv/query_service/latest-mediainfo.ttl.bz2',
    }
}


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
    parser.add_argument('--reuse-downloaded-dump', action='store_true', help='Reuse downloaded dump')
    parser.add_argument('--downtime', type=int, default=336, help='Hour(s) of downtime')
    parser.add_argument('--depool', action='store_true', help='Should be depooled.')
    parser.add_argument('--reload-data', required=True, choices=['wikidata', 'categories', 'commons'],
                        help='Type of data to reload')
    parser.add_argument('--kafka-timestamp', type=int, help='Timestamp to use for kafka consumer topic reset (in ms)')

    return parser


def get_dumps(dumps, remote_host, proxy_server, reuse_dump):
    """Use dump file if present else download file."""
    if proxy_server:
        curl_command = "curl -x {proxy_server}".format(proxy_server=proxy_server)
    else:
        curl_command = "curl"

    for dump in dumps:
        if reuse_dump:
            try:
                remote_host.run_sync("test -f {path}".format(path=dump['path']), is_safe=True)
                logger.info('Detected dump (%s). Skipping download', dump['path'])
                continue
            except RemoteExecutionError:
                logger.info('Dump (%s) not found', dump['path'])

        file = os.path.basename(dump['path'])
        logger.info('Downloading (%s)', file)
        watch = StopWatch()
        remote_host.run_sync(
            "{curl_command} {url} -o {path}".format(
                curl_command=curl_command, url=dump['url'], path=dump['path'])
        )
        logger.info('Downloaded %s in %s', file, watch.elapsed())


def fail_for_disk_space(remote_host, dumps, journal_path):
    """Available disk space must be 2.5x greater than dump file."""
    logger.info("checking available disk space")
    dump_paths = ' '.join(dump['path'] for dump in dumps)
    remote_host.run_sync(
        "dump_size=`du --total {dump_paths} | tail -n 1 | cut -f1` && "
        "db_size=`du {journal_path} | cut -f1` && "
        "disk_avail=`df --output=avail {journal_path} | tail -1` && "
        "test $(($dump_size*5/2)) -lt $(($disk_avail+$db_size))".format(
            dump_paths=dump_paths, journal_path=journal_path), is_safe=True)


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
            .format(path=dump['path'],
                    munge_path=dump['munge_path'],
                    munge_jar_args=dump.get('munge_jar_args', ''))
        )
        logger.info('munging %s completed in %s', dump['munge_path'], stop_watch.elapsed())


def reload_commons(remote_host, puppet, kafka, timestamps, consumer_definition, reason):
    """Execute commands on host to reload commons data."""
    logger.info('Prepare to load commons data for blazegraph')
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
            munge_path=DUMPS['commons']['munge_path']
        )
    )
    logger.info('Commons dump loaded in %s', watch.elapsed())
    kafka.set_consumer_position_by_timestamp(consumer_definition, timestamps)
    remote_host.run_sync(
        'touch /srv/query_service/data_loaded',
        'systemctl start wcqs-updater'
    )


def reload_wikidata(remote_host, puppet, kafka, timestamps, consumer_definition, reason):
    """Execute commands on host to reload wikidata data."""
    logger.info('Prepare to load wikidata data for blazegraph')
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
            munge_path=DUMPS['wikidata']['munge_path']
        )
    )
    logger.info('Wikidata dump loaded in %s', watch.elapsed())
    logger.info('Loading lexeme dump')
    watch.reset()
    remote_host.run_sync(
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wdq -d {munge_path}".format(
            munge_path=DUMPS['lexeme']['munge_path']
        )
    )
    logger.info('Lexeme dump loaded in %s', watch.elapsed())
    logger.info('Performing final steps')
    kafka.set_consumer_position_by_timestamp(consumer_definition, timestamps)
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

    def fetch_dumps(dumps, journal):
        get_dumps(dumps, remote_host, args.proxy_server, args.reuse_downloaded_dump)
        fail_for_disk_space(remote_host, dumps, journal)
        munge(dumps, remote_host)

    dumps = []
    if 'wikidata' == args.reload_data:
        dumps = [DUMPS['wikidata'], DUMPS['lexeme']]
        fetch_dumps(dumps, '/srv/wdqs/wikidata.jnl')
    if 'commons' == args.reload_data:
        dumps = [DUMPS['commons']]
        fetch_dumps(dumps, '/srv/query_service/wcqs.jnl')

    if args.reload_data in ['wikidata', 'commons'] and args.kafka_timestamp is None:
        raise ValueError("--kafka-timestamp should be set when reloading commons or wikidata")

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
        reload_fn(remote_host, puppet, spicerack.kafka(), {mutation_topic: args.kafka_timestamp},
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

    if dumps:
        logger.info('Cleaning up downloads')
        dump_paths = " ".join(dump['path'] for dump in dumps)
        remote_host.run_sync("rm {dump_paths}".format(dump_paths=dump_paths))
