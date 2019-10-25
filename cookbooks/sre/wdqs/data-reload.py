"""WDQS data reload

Usage example:
    cookbook sre.wdqs.data-reload wdqs1010.eqiad.wmnet --reason 'fix issues' --task-id T12345

"""

import argparse
import logging

from contextlib import contextmanager
from datetime import timedelta

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


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('host', help='select a single WDQS host.')
    parser.add_argument('--task-id', help='task id for the change')
    parser.add_argument('--proxy-server', help='Specify proxy server to use')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--reuse-downloaded-dump', action='store_true', help='Reuse downloaded dump')
    parser.add_argument('--downtime', type=int, default=1, help='Hour(s) of downtime')
    parser.add_argument('--depool', action='store_true', help='Should be depooled.')

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
        remote_host.run_sync(
            "{curl_command} https://dumps.wikimedia.your.org/wikidatawiki/entities/{file} -o {path}".format(
                curl_command=curl_command, file=dump['file'], path=dump['path'])
        )


def fail_for_disk_space(remote_host):
    """Available disk space must be 2.5x greater than dump file."""
    logger.info("checking available disk space")
    remote_host.run_sync(
        "dump_size=`du {path} | cut -f1` && "
        "db_size=`du /srv/wdqs/wikidata.jnl | cut -f1` && "
        "disk_avail=`df --output=avail /srv | tail -1` && "
        "test $(($dump_size*5/2)) -lt $(($disk_avail+$db_size))".format(
            path=WDQS_DUMPS['wikidata']['path']), is_safe=True)


def munge(remote_host):
    """Run munger for main database and lexeme"""
    logger.info('Running munger for main database and then lexeme')
    for dump in WDQS_DUMPS.values():
        remote_host.run_sync(
            "mkdir -p {munge_path} && bash /srv/deployment/wdqs/wdqs/munge.sh -f {path} -d {munge_path}".format(
                path=dump['path'], munge_path=dump['munge_path']),
        )


@retry(tries=48, delay=timedelta(minutes=5), backoff_mode='constant', exceptions=(ValueError,))
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


def reload_data(remote_host):
    """Execute commands on host to reload data."""
    logger.info('Prepare to load data for blazegraph')
    remote_host.run_sync(
        'rm -v /srv/wdqs/data_loaded',
        'systemctl stop wdqs-updater',
        'systemctl stop wdqs-blazegraph',
        'rm -v /srv/wdqs/wikidata.jnl'
    )

    logger.info('Loading wikidata dump')
    remote_host.run_sync(
        'systemctl start wdqs-blazegraph',
        'sleep 60',
        'test -f /srv/wdqs/wikidata.jnl',
        "bash /srv/deployment/wdqs/wdqs/loadData.sh -n wdq -d {munge_path}".format(
            munge_path=WDQS_DUMPS['wikidata']['munge_path']
        )
    )

    logger.info('Loading lexeme dump')
    remote_host.run_sync(
        'curl -XPOST --data-binary update="LOAD <file:///{munge_path}/wikidump-000000001.ttl.gz>" '
        'http://localhost:9999/bigdata/namespace/wdq/sparql'.format(
            munge_path=WDQS_DUMPS['lexeme']['munge_path'])
    )

    logger.info('Performing final steps')
    remote_host.run_sync(
        'touch /srv/wdqs/data_loaded',
        'systemctl start wdqs-updater'
    )

    logger.info('Preparing to load data for categories')
    remote_host.run_sync(
        'systemctl stop wdqs-categories',
        'rm /srv/wdqs/categories.jnl'
    )

    logger.info('Loading data for categories')
    remote_host.run_sync(
        'systemctl start wdqs-categories',
        'sleep 30',
        'test -f /srv/wdqs/categories.jnl',
        '/usr/local/bin/reloadCategories.sh'
    )

    logger.info('Cleaning up downloads')
    dump_paths = " ".join([dump['path'] for dump in WDQS_DUMPS.values()])
    remote_host.run_sync("rm {dump_paths}".format(dump_paths=dump_paths))


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

    get_dumps(remote_host, args.proxy_server, args.reuse_downloaded_dump)
    fail_for_disk_space(remote_host)
    munge(remote_host)

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
        with puppet.disabled(reason):
            with depool_host():
                remote_host.run_sync('sleep 180')
                reload_data(remote_host)

                logger.info('Data reload for blazegraph is complete. Waiting for updater to catch up')
                wait_for_updater(prometheus, args.site, remote_host)
