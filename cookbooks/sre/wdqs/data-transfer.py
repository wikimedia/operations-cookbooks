"""WDQS data transfer cookbook for source node

Usage example for hosts behind lvs:
    cookbook sre.wdqs.data-transfer --source wdqs1004.eqiad.wmnet --dest wdqs1003.eqiad.wmnet \
    --lvs-strategy both --reason "allocator troubles" --blazegraph_instance wdqs-all --task-id T12345

Usage example for test hosts (not lvs managed):
    cookbook sre.wdqs.data-transfer --source wdqs1009.eqiad.wmnet --dest wdqs1010.eqiad.wmnet \
    --lvs-strategy neither --reason "moving away from legacy updater" --blazegraph_instance wdqs-all --task-id T12345

"""
import argparse
import logging

from typing import cast
from datetime import timedelta
from time import sleep

import transferpy.transfer
from transferpy.Transferer import Transferer

from spicerack.kafka import ConsumerDefinition

from cookbooks.sre.wdqs import check_hosts_are_valid, wait_for_updater, get_site, get_hostname, MUTATION_TOPICS

BLAZEGRAPH_INSTANCES = {

    'categories': {
        'services': ['wdqs-categories'],
        'data_path': '/srv/wdqs',
        'files': ['/srv/wdqs/categories.jnl', '/srv/wdqs/aliases.map'],
        'valid_on': 'wdqs',
    },
    'wikidata': {
        'services': ['wdqs-updater', 'wdqs-blazegraph'],
        'data_path': '/srv/wdqs',
        'files': ['/srv/wdqs/wikidata.jnl'],
        'valid_on': 'wdqs',
    },
    'commons': {
        'services': ['wcqs-updater', 'wcqs-blazegraph'],
        'data_path': '/srv/query_service',
        'files': ['/srv/query_service/wcqs.jnl'],
        'valid_on': 'wcqs',
    },
}

LVS_STRATEGY = ['neither', 'source-only', 'dest-only', 'both']

__title__ = "WDQS data transfer cookbook"
logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for all the sre.wdqs cookbooks."""
    parser = argparse.ArgumentParser(prog=__name__, description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--source', required=True, help='FQDN of source node.')
    parser.add_argument('--dest', required=True, help='FQDN of destination node.')
    parser.add_argument('--blazegraph_instance', required=True, choices=list(BLAZEGRAPH_INSTANCES.keys()) +
                        ['wdqs-all'], help='One of: %(choices)s.')
    parser.add_argument('--reason', required=True, help='Administrative Reason')
    parser.add_argument('--downtime', type=int, default=6, help="Hours of downtime")
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--lvs-strategy', required=True, help='which hosts to depool/repool', choices=LVS_STRATEGY)
    parser.add_argument('--encrypt', action='store_true', help='Enable encryption on transfer')
    parser.add_argument('--force', action='store_true', help='Delete files on target before transfer')

    return parser


def _transfer_datadir(source, path, files, dest, encrypt):
    """Transfer WDQS data using transferpy library."""
    # Read transferpy config from /etc/transferpy/transferpy.conf,
    # which is present on cumin hosts.
    tp_opts = dict(transferpy.transfer.parse_configurations(transferpy.transfer.CONFIG_FILE))
    # this also handles string->bool conversion where necessary
    tp_opts = transferpy.transfer.assign_default_options(tp_opts)
    tp_opts['verbose'] = True
    tp_opts['encrypt'] = encrypt
    logger.debug("Creating transfer object with args: %s %s %s %s", path, source, files, dest)
    # wdqs-categories has 2 files, so we need a loop.
    for file in files:
        xfer_object = Transferer(source, file, [dest], [path], tp_opts)
        # The below linting error seems to be a false positive. Disable.
        # pylint:disable=unused-variable
        xfer = xfer_object.run()  # noqa: F841


def run(args, spicerack):
    """Run the data transfer on each indicated instance."""
    if args.blazegraph_instance == 'wdqs-all':
        run_for_instance(args, spicerack, 'wikidata', BLAZEGRAPH_INSTANCES['wikidata'])
        run_for_instance(args, spicerack, 'categories', BLAZEGRAPH_INSTANCES['categories'])
    else:
        run_for_instance(args, spicerack, args.blazegraph_instance, BLAZEGRAPH_INSTANCES[args.blazegraph_instance])


def _pool_host(host_type, host):
    """Pool the source or dest host"""
    logger.info('pooling %s host %s', host_type, host)
    host.run_sync('pool')
    sleep(120)


def _depool_host(host_type, host):
    """Depool the source or dest host"""
    logger.info('depooling %s host %s', host_type, host)
    host.run_sync('depool')
    sleep(120)


def lvs_action(action_func, lvs_strategy, source, dest):
    """Decide which hosts to operate on"""
    # Use lvs_strategy to decide hosts to target
    if lvs_strategy == "both":
        action_func('source', source)
        action_func('dest', dest)
    elif lvs_strategy == "source-only":
        action_func('source', source)
    elif lvs_strategy == "dest-only":
        action_func('dest', dest)


def run_for_instance(args, spicerack, bg_instance_name, instance):
    # pylint:disable=too-many-locals
    """Required by Spicerack API."""
    remote = spicerack.remote()
    remote_hosts = remote.query("{source},{dest}".format(source=args.source, dest=args.dest))
    host_kind = check_hosts_are_valid(remote_hosts, remote)
    if host_kind != instance['valid_on']:
        raise ValueError('Instance (valid_on:{}) is not valid for selected hosts ({})'.format(
            instance['valid_on'], host_kind))

    alerting_hosts = spicerack.alerting_hosts(remote_hosts.hosts)
    puppet = spicerack.puppet(remote_hosts)
    prometheus = spicerack.prometheus()
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

    source = remote.query(args.source)
    dest = remote.query(args.dest)

    for argument in source, dest:
        if len(argument) != 1:
            raise ValueError("Only one argument is needed. Not {total}({argument})".
                             format(total=len(argument), argument=argument))

    services = cast(list, instance['services'])
    files = instance['files']

    stop_services_cmd = " && ".join(["systemctl stop " + service for service in services])
    services.reverse()
    start_services_cmd = " && sleep 10 && ".join(["systemctl start " + service for service in services])

    alerting_hosts.downtime(reason, duration=timedelta(hours=args.downtime))

    with puppet.disabled(reason):
        lvs_action(_depool_host, args.lvs_strategy, source, dest)

        logger.info('Stopping services [%s]', stop_services_cmd)
        remote_hosts.run_sync(stop_services_cmd)

        data_path = instance['data_path']

        if args.force:
            for file in files:
                dest.run_sync('rm -fv {}'.format(file))

            dest.run_sync('rm -fv /srv/wdqs/data_loaded')

        _transfer_datadir(args.source, data_path, files, args.dest, args.encrypt)

        for file in files:
            dest.run_sync('chown blazegraph: "{file}"'.format(file=file))

        if bg_instance_name not in ('commons'):
            logger.info('Touching "data_loaded" file to show that data load is completed.')
            dest.run_sync('touch {data_path}/data_loaded'.format(
                data_path=data_path))

        if bg_instance_name == 'categories':
            logger.info('Reloading nginx to load new categories mapping.')
            dest.run_sync('systemctl reload nginx')

        source_hostname = get_hostname(args.source)
        dest_hostname = get_hostname(args.dest)

        if bg_instance_name in MUTATION_TOPICS:
            logger.info('Transferring Kafka offsets')
            kafka = spicerack.kafka()
            kafka.transfer_consumer_position([MUTATION_TOPICS[bg_instance_name]],
                                             ConsumerDefinition(get_site(source_hostname, spicerack), 'main',
                                                                source_hostname),
                                             ConsumerDefinition(get_site(dest_hostname, spicerack), 'main',
                                                                dest_hostname))

        logger.info('Starting services [%s]', start_services_cmd)
        remote_hosts.run_sync(start_services_cmd)

        if bg_instance_name in MUTATION_TOPICS:
            wait_for_updater(prometheus, get_site(source_hostname, spicerack), source)
            wait_for_updater(prometheus, get_site(dest_hostname, spicerack), dest)

        lvs_action(_pool_host, args.lvs_strategy, source, dest)
