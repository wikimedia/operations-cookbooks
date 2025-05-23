"""WDQS data transfer cookbook for source node

Usage example for hosts behind lvs:
    cookbook sre.wdqs.data-transfer --source wdqs1004.eqiad.wmnet --dest wdqs1003.eqiad.wmnet \
    --lvs-strategy both --reason "allocator troubles" --blazegraph_instance wdqs-all --task-id T12345

Usage example for test hosts (not lvs managed):
    cookbook sre.wdqs.data-transfer --source wdqs1009.eqiad.wmnet --dest wdqs1010.eqiad.wmnet \
    --lvs-strategy neither --reason "moving away from legacy updater" --blazegraph_instance wdqs-all --task-id T12345

"""
import logging

from typing import cast
from datetime import timedelta
from time import sleep

import transferpy.transfer
from transferpy.Transferer import Transferer

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.kafka import ConsumerDefinition

from cookbooks.sre.wdqs import wait_for_updater, get_site, get_hostname, MUTATION_TOPICS

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
    'wikidata_full': {
        'services': ['wdqs-updater', 'wdqs-blazegraph'],
        'data_path': '/srv/wdqs',
        'files': ['/srv/wdqs/wikidata.jnl'],
        'valid_on': 'wdqs',
    },
    'wikidata_main': {
        'services': ['wdqs-updater', 'wdqs-blazegraph'],
        'data_path': '/srv/wdqs',
        'files': ['/srv/wdqs/wikidata.jnl'],
        'valid_on': 'wdqs',
    },
    'scholarly_articles': {
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
logger = logging.getLogger(__name__)


class DataTransfer(CookbookBase):
    """Transfer journal files from one host to another.

    Use lvs-strategy to determine which hosts to depool & repool.
    Use force when wanting to delete existing jnl files.
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self):
        """Parse the command line arguments for all the sre.wdqs cookbooks."""
        parser = super().argument_parser()

        parser.add_argument('--source', required=True, help='FQDN of source node.')
        parser.add_argument('--dest', required=True, help='FQDN of destination node.')
        parser.add_argument('--blazegraph_instance', required=True, choices=list(BLAZEGRAPH_INSTANCES.keys()) +
                            ['wdqs-all'], help='One of: %(choices)s.')
        parser.add_argument('--downtime', type=int, default=6, help="Hours of downtime")
        parser.add_argument('--lvs-strategy', required=True, help='which hosts to depool/repool', choices=LVS_STRATEGY)
        parser.add_argument('--encrypt', action='store_true', help='Enable encryption on transfer')
        parser.add_argument('--force', action='store_true', help='Delete files on target before transfer')
        parser.add_argument('--no-check-graph-type', action='store_true', help="Don't check hosts have same graph type"
                            " (use this for initial host setup)")

        return parser

    def get_runner(self, args):
        """Create runner object to perform data transfer."""
        return DataTransferRunner(args, self.spicerack)


# pylint: disable= too-many-instance-attributes
# TODO: some more in depth refactoring of this class might be needed to reduce the number of instance attributes
class DataTransferRunner(CookbookRunnerBase):
    """Transfer journal files from one host to another."""

    def __init__(self, args, spicerack):
        """Unpack and sanity-check args & store in self."""
        self.remote = spicerack.remote()

        self.r_source = self.remote.query(args.source)
        self.r_dest = self.remote.query(args.dest)

        for argument in self.r_source, self.r_dest:
            if len(argument) != 1:
                raise ValueError("Only one argument is needed. Not {total}({argument})".
                                 format(total=len(argument), argument=argument))

        self.remote_hosts = self.remote.query("{source},{dest}".format(source=self.r_source, dest=self.r_dest))

        self.blazegraph_instance = args.blazegraph_instance
        self.reason = args.reason
        self.downtime = args.downtime
        self.task_id = args.task_id
        self.lvs_strategy = args.lvs_strategy
        self.encrypt = args.encrypt
        self.force = args.force
        self.no_check_graph_type = args.no_check_graph_type

        self.prometheus = spicerack.prometheus()
        self.kafka = spicerack.kafka()
        self.netbox = spicerack.netbox()

        self.alerting_hosts = spicerack.alerting_hosts
        self.puppet = spicerack.puppet

        self.admin_reason = spicerack.admin_reason(self.reason, task_id=self.task_id)

    def run(self):
        """Run the data transfer on each indicated instance."""
        if self.blazegraph_instance == 'wdqs-all':
            self.run_for_instance('wikidata', BLAZEGRAPH_INSTANCES['wikidata'])
            self.run_for_instance('categories', BLAZEGRAPH_INSTANCES['categories'])
        else:
            self.run_for_instance(self.blazegraph_instance, BLAZEGRAPH_INSTANCES[self.blazegraph_instance])

    @property
    def runtime_description(self):
        """Return a string that represents which operation will be performed as well as the target cluster + reason."""
        msg = f"({self.task_id}, {self.reason}) xfer {self.blazegraph_instance} from {self.r_source} -> {self.r_dest}"
        if self.encrypt:
            msg += " w/ encryption"
        if self.encrypt and self.force:
            msg += " and"
        if self.force:
            msg += " w/ force delete existing files"

        msg += f", repooling {self.lvs_strategy} afterwards"

        return msg

    def transfer_datafiles(self, path, files):
        """Transfer WDQS data using transferpy library."""
        # Read transferpy config from /etc/transferpy/transferpy.conf,
        # which is present on cumin hosts.
        tp_opts = dict(transferpy.transfer.parse_configurations(transferpy.transfer.CONFIG_FILE))
        # this also handles string->bool conversion where necessary
        tp_opts = transferpy.transfer.assign_default_options(tp_opts)
        tp_opts['verbose'] = True
        tp_opts['encrypt'] = self.encrypt
        logger.debug("Creating transfer object with args: %s %s %s %s", path, self.r_source, files, self.r_dest)

        for file in files:
            Transferer(str(self.r_source), file, [str(self.r_dest)], [path], tp_opts).run()

    @staticmethod
    def _pool_host(host_type, host):
        """Pool the source or dest host"""
        logger.info('pooling %s host %s', host_type, host)
        host.run_sync('pool')

    @staticmethod
    def _depool_host(host_type, host):
        """Depool the source or dest host"""
        logger.info('depooling %s host %s', host_type, host)
        host.run_sync('depool')

    @staticmethod
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

        if lvs_strategy != "neither":
            logger.info('sleeping for 120s')  # TODO poll instead of sleep
            sleep(120)

    @staticmethod
    def get_graph_type_from_host(remote_host, data_loaded_flag_filepath):
        """Given a remote_host and string representing data loaded filepath, return graph type as string"""
        graph_type_output = remote_host.run_sync(f'cat {data_loaded_flag_filepath}')
        for _, output in graph_type_output:
            graph_type = output.message().decode().splitlines()[0]  # TODO: try catch or something maybe
            logger.info('found graph_type of %s', graph_type)
            return graph_type
        raise ValueError("Failed to extract graph_type")

    def run_for_instance(self, bg_instance_name, instance):
        """Required by Spicerack API."""
        # Check graph type on source and dest
        data_loaded_flag_filepath = instance['data_path'] + '/data_loaded'
        if not self.no_check_graph_type:
            source_graph_type = DataTransferRunner.get_graph_type_from_host(self.r_source, data_loaded_flag_filepath)
            dest_graph_type = DataTransferRunner.get_graph_type_from_host(self.r_dest, data_loaded_flag_filepath)
            if source_graph_type != dest_graph_type:
                raise ValueError("source host {} has graph type of {} but dest host {} has graph type of {}, aborting"
                                 .format(self.r_source, source_graph_type, self.r_dest, dest_graph_type))
            logger.info('Both hosts have graph type of %s, proceeding', source_graph_type)

        alerting_hosts = self.alerting_hosts(self.remote_hosts.hosts)

        services = cast(list, instance['services'])
        files = instance['files']
        if bg_instance_name != 'categories':
            files.append(data_loaded_flag_filepath)
        logger.info("Decided on ultimately transferring the following files: %s", files)

        stop_services_cmd = " && ".join(["systemctl stop " + service for service in services])
        services.reverse()
        start_services_cmd = " && sleep 10 && ".join(["systemctl start " + service for service in services])

        with alerting_hosts.downtimed(self.admin_reason, duration=timedelta(hours=self.downtime)):
            with self.puppet(self.remote_hosts).disabled(self.admin_reason):
                DataTransferRunner.lvs_action(DataTransferRunner._depool_host, self.lvs_strategy, self.r_source,
                                              self.r_dest)

                logger.info('Stopping services [%s]', stop_services_cmd)
                self.remote_hosts.run_sync(stop_services_cmd)

                data_path = instance['data_path']

                if self.force:
                    for file in files:
                        self.r_dest.run_sync('rm -fv {}'.format(file))

                    self.r_dest.run_sync('rm -fv /srv/wdqs/data_loaded')

                self.transfer_datafiles(data_path, files)

                for file in files:
                    self.r_dest.run_sync('chown blazegraph: "{file}"'.format(file=file))

                if bg_instance_name not in ('categories'):
                    logger.info('Setting file %s/data_loaded to %s', data_path, bg_instance_name)
                    self.r_dest.run_sync('echo {bg_instance_name} > {data_path}/data_loaded'.format(
                        bg_instance_name=bg_instance_name, data_path=data_path))

                if bg_instance_name == 'categories':
                    logger.info('Reloading nginx to load new categories mapping')
                    self.r_dest.run_sync('systemctl reload nginx')

                source_hostname = get_hostname(str(self.r_source))
                dest_hostname = get_hostname(str(self.r_dest))

                if bg_instance_name in MUTATION_TOPICS:
                    logger.info('Transferring Kafka offsets')
                    self.kafka.transfer_consumer_position([MUTATION_TOPICS[bg_instance_name]],
                                                          ConsumerDefinition(get_site(source_hostname, self.netbox),
                                                                             'main',
                                                                             source_hostname),
                                                          ConsumerDefinition(get_site(dest_hostname, self.netbox),
                                                                             'main',
                                                                             dest_hostname))

                logger.info('Starting services [%s]', start_services_cmd)
                self.remote_hosts.run_sync(start_services_cmd)

                if bg_instance_name in MUTATION_TOPICS:
                    wait_for_updater(self.prometheus, get_site(source_hostname, self.netbox), self.r_source)
                    wait_for_updater(self.prometheus, get_site(dest_hostname, self.netbox), self.r_dest)

                DataTransferRunner.lvs_action(DataTransferRunner._pool_host,
                                              self.lvs_strategy, self.r_source, self.r_dest)
