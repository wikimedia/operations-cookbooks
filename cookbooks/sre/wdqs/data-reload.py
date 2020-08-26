"""WDQS data reload

Usage example:
    cookbook sre.wdqs.data-reload wdqs1010.eqiad.wmnet --reason 'fix issues' --task-id T12345

"""

import argparse
import logging

from contextlib import contextmanager
from datetime import datetime, timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError

from . import check_host_is_wdqs

__title__ = "WDQS data reload cookbook"
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

WDQS_DUMPS = {
    'wikidata': {
        'file': 'latest-all.ttl.bz2',
        'munge_path': '/srv/wdqs/munged',
        'path': '/srv/wdqs/latest-all.ttl.bz2',
    },
    'lexeme': {
        'file': 'latest-lexemes.ttl.bz2',
        'munge_path': '/srv/wdqs/lex-munged',
        'path': '/srv/wdqs/latest-lexemes.ttl.bz2',
    }
}

RELOAD_TYPES = {
    'all': ['wikidata', 'categories'],
    'wikidata': ['wikidata'],
    'categories': ['categories']
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
    parser.add_argument('--reload-data', default='all', choices=RELOAD_TYPES.keys(),
                        help='Type of data to reload')
    parser.add_argument('--skolemize', action='store_true', help='Skolemize blank nodes when munging')

    return parser


def get_dumps(remote_host, proxy_server, reuse_dump):
    """Use dump file if present else download file."""
    if proxy_server:
        curl_command = "curl -x {proxy_server}".format(proxy_server=proxy_server)
    else:
        curl_command = "curl"

    for dump in WDQS_DUMPS.values():
        if reuse_dump:
            try:
                remote_host.run_sync("test -f {path}".format(path=dump['path']), is_safe=True)
                logger.info('Detected dump (%s). Skipping download', dump['path'])
                continue
            except RemoteExecutionError:
                logger.info('Dump (%s) not found', dump['path'])

        logger.info('Downloading (%s)', dump['file'])
        watch = StopWatch()
        remote_host.run_sync(
            "{curl_command} https://dumps.wikimedia.your.org/wikidatawiki/entities/{file} -o {path}".format(
                curl_command=curl_command, file=dump['file'], path=dump['path'])
        )
        logger.info('Downloaded %s in %s', dump['file'], watch.elapsed())


def fail_for_disk_space(remote_host):
    """Available disk space must be 2.5x greater than dump file."""
    logger.info("checking available disk space")
    remote_host.run_sync(
        "dump_size=`du {path} | cut -f1` && "
        "db_size=`du /srv/wdqs/wikidata.jnl | cut -f1` && "
        "disk_avail=`df --output=avail /srv | tail -1` && "
        "test $(($dump_size*5/2)) -lt $(($disk_avail+$db_size))".format(
            path=WDQS_DUMPS['wikidata']['path']), is_safe=True)


def munge(remote_host, skolemize):
    """Run munger for main database and lexeme"""
    logger.info('Running munger for main database and then lexeme')
    stop_watch = StopWatch()
    for dump in WDQS_DUMPS.values():
        logger.info('munging %s (skolemizaton: %s)', dump['munge_path'], str(skolemize))
        stop_watch.reset()
        remote_host.run_sync(
            "rm -rf {munge_path} && mkdir -p {munge_path} && bzcat {path} | "
            "/srv/deployment/wdqs/wdqs/munge.sh -f - -d {munge_path} -- {skolemize}"
            .format(path=dump['path'], munge_path=dump['munge_path'], skolemize="--skolemize" if skolemize else ""),
        )
        logger.info('munging %s completed in %s', dump['munge_path'], stop_watch.elapsed())


@retry(tries=1000, delay=timedelta(minutes=10), backoff_mode='constant', exceptions=(ValueError,))
def wait_for_updater(prometheus, site, remote_host):
    """Wait for wdqs updater to catch up on updates.

    This might take a while to complete and its completely normal.
    Hence, the long wait time.
    """
    host = remote_host.hosts[0].split(".")[0]
    query = "scalar(time() - blazegraph_lastupdated{instance='%s:9193'})" % host
    result = prometheus.query(query, site)
    last_updated = int(result['value'][1])
    if last_updated > 1200:
        raise ValueError("Let's wait for updater to catch up.")


def reload_wikidata(remote_host):
    """Execute commands on host to reload wikidata data."""
    logger.info('Prepare to load wikidata data for blazegraph')
    remote_host.run_sync(
        'rm -v /srv/wdqs/data_loaded',
        'systemctl stop wdqs-updater',
        'systemctl stop wdqs-blazegraph',
        'rm -v /srv/wdqs/wikidata.jnl'
    )

    logger.info('Loading wikidata dump')
    watch = StopWatch()
    remote_host.run_sync(
        'systemctl start wdqs-blazegraph',
        'sleep 60',
        'test -f /srv/wdqs/wikidata.jnl',
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wdq -d {munge_path}".format(
            munge_path=WDQS_DUMPS['wikidata']['munge_path']
        )
    )
    logger.info('Wikidata dump loaded in %s', watch.elapsed())

    logger.info('Loading lexeme dump')
    watch.reset()
    remote_host.run_sync(  # FIXME missing loop for multiple files
        'curl -XPOST --data-binary update="LOAD <file:///{munge_path}/wikidump-000000001.ttl.gz>" '
        'http://localhost:9999/bigdata/namespace/wdq/sparql'.format(
            munge_path=WDQS_DUMPS['lexeme']['munge_path'])
    )
    logger.info('Lexeme dump loaded in %s', watch.elapsed())

    logger.info('Performing final steps')
    remote_host.run_sync(
        'touch /srv/wdqs/data_loaded',
        'systemctl start wdqs-updater'
    )

    logger.info('Cleaning up downloads')
    dump_paths = " ".join([dump['path'] for dump in WDQS_DUMPS.values()])
    remote_host.run_sync("rm {dump_paths}".format(dump_paths=dump_paths))


def reload_categories(remote_host):
    """Execute commands on host to reload categories data."""
    logger.info('Preparing to load data for categories')
    remote_host.run_sync(
        'systemctl stop wdqs-categories',
        'rm /srv/wdqs/categories.jnl'
    )

    logger.info('Loading data for categories')
    watch = StopWatch()
    remote_host.run_sync(
        'systemctl start wdqs-categories',
        'sleep 30',
        'test -f /srv/wdqs/categories.jnl',
        '/usr/local/bin/reloadCategories.sh wdqs'
    )
    logger.info('Categories loaded in %s', watch.elapsed())


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_host = remote.query(args.host)
    check_host_is_wdqs(remote_host, remote)

    if len(remote_host) != 1:
        raise ValueError("Only one host is needed. Not {total}({source})".
                         format(total=len(remote_host), source=remote_host))

    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_host)
    prometheus = spicerack.prometheus()
    confctl = spicerack.confctl('node')
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    data_to_reload = RELOAD_TYPES[args.reload_data]

    if 'wikidata' in data_to_reload:
        get_dumps(remote_host, args.proxy_server, args.reuse_downloaded_dump)
        fail_for_disk_space(remote_host)
        munge(remote_host, args.skolemize)

    @contextmanager
    def noop_change_and_revert():
        yield

    def change_and_revert():
        return confctl.change_and_revert('pooled', True, False, name=remote_host.hosts[0])

    if args.depool:
        depool_host = change_and_revert
    else:
        depool_host = noop_change_and_revert

    with icinga.hosts_downtimed(remote_host.hosts, reason, duration=timedelta(hours=args.downtime)):
        # FIXME: this cookbook is expected to run for weeks, we don't want to disable puppet for that long
        #        but we want to ensure that the various services aren't restarted by puppet along the way.
        with puppet.disabled(reason):
            with depool_host():
                remote_host.run_sync('sleep 180')
                if 'categories' in data_to_reload:
                    reload_categories(remote_host)

                if 'wikidata' in data_to_reload:
                    reload_wikidata(remote_host)

                    logger.info('Data reload for blazegraph is complete. Waiting for updater to catch up')
                    watch = StopWatch()
                    wait_for_updater(prometheus, args.site, remote_host)
                    logger.info('Catch up on updates in %s', watch.elapsed())
