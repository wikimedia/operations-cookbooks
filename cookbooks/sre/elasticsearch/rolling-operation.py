"""Perform rolling operations on elasticsearch servers"""
# flake8: noqa: E501
import argparse
import logging
from cookbooks import ArgparseFormatter
from datetime import datetime, timedelta
from enum import Enum, auto
from time import sleep

from spicerack.constants import CORE_DATACENTERS
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from cookbooks.sre.elasticsearch import CLUSTERGROUPS, valid_datetime_type

logger = logging.getLogger(__name__)


class Operation(Enum):
    RESTART = auto()
    REBOOT = auto()
    UPGRADE = auto()


class RollingOperation(CookbookBase):
    """
    Perform a rolling operation on a CirrusSearch Elasticsearch cluster.
    Will perform Elasticsearch service restarts by default.
    Optionally perform a full reboot instead of just service restarts,
    and additionally can optionally perform a plugin upgrade in addition to the restart or reboot.

    Usage examples:
        (Perform a rolling restart of eqiad)
        cookbook sre.elasticsearch.rolling-operation search_eqiad "eqiad cluster restart" --nodes-per-run 3 --start-datetime 2021-03-24T23:55:35 --task-id T274204

        (Perform a rolling reboot of codfw)
        cookbook sre.elasticsearch.rolling-operation search_codfw "codfw cluster reboot" --reboot --nodes-per-run 3 --start-datetime 2021-03-24T23:55:35 --task-id T274204

        (Perform a plugin upgrade followed by rolling restart of relforge)
        cookbook sre.elasticsearch.rolling-operation relforge "relforge plugin upgrade + reboot" --reboot --upgrade --nodes-per-run 3 --start-datetime 2021-03-24T23:55:35 --task-id T274204
    """
    def argument_parser(self):
        """Parse the command line arguments for rolling operations (restart/reboot/upgrade).

        TODO:
            Remove ``--without-lvs`` for a better implementation as this was introduced because
            relforge cluster does not have lvs enabled.

        """
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=ArgparseFormatter)
        parser.add_argument('clustergroup', choices=CLUSTERGROUPS,
                            help='Name of clustergroup. One of: %(choices)s.')
        parser.add_argument('admin_reason', help='Administrative Reason')
        parser.add_argument('--start-datetime', type=valid_datetime_type,
                            help='start datetime in ISO 8601 format e.g 2018-09-15T15:53:00')
        parser.add_argument('--task-id', help='task_id for the change')
        parser.add_argument('--nodes-per-run', default=3, type=int, help='Number of nodes per run.')
        parser.add_argument('--without-lvs', action='store_false', dest='with_lvs',
                            help='This cluster does not use LVS.')
        parser.add_argument('--no-wait-for-green', action='store_false', dest='wait_for_green',
                            help='Don\'t wait for green before starting the operation (still wait at the end).')
        parser.add_argument('--upgrade', action='store_true',
                            help='Perform a plugin upgrade as part of the rolling operation')
        parser.add_argument('--reboot', action='store_true',
                            help='Perform a full reboot [rather than only service restarts]')
        parser.add_argument('--write-queue-datacenters', choices=CORE_DATACENTERS, default=CORE_DATACENTERS, nargs='+',
                            help='Manually specify a list of specific datacenters to check the '
                                 'cirrus write queue rather than checking all core datacenters (default)')

        return parser

    def get_runner(self, args):
        if args.start_datetime is None:
            args.start_datetime = datetime.utcnow()

        clustergroup = args.clustergroup
        write_queue_datacenters = args.write_queue_datacenters
        elasticsearch_clusters = self.spicerack.elasticsearch_clusters(
            clustergroup, write_queue_datacenters)

        reason = self.spicerack.admin_reason(args.admin_reason, task_id=args.task_id)
        start_datetime = args.start_datetime
        nodes_per_run = args.nodes_per_run
        with_lvs = args.with_lvs
        wait_for_green = args.wait_for_green
        operations = list()
        if args.upgrade:
            operations.append(Operation.UPGRADE)
        if args.reboot:
            operations.append(Operation.REBOOT)
        else:
            operations.append(Operation.RESTART)

        return RollingOperationRunner(
            self.spicerack, elasticsearch_clusters, clustergroup, reason, start_datetime,
            nodes_per_run, with_lvs, wait_for_green, operations)


class RollingOperationRunner(CookbookRunnerBase):
    def __init__(self, spicerack, elasticsearch_clusters, clustergroup, reason, start_datetime,
                 nodes_per_run, with_lvs, wait_for_green, operations):
        self.spicerack = spicerack

        self.elasticsearch_clusters = elasticsearch_clusters
        self.clustergroup = clustergroup
        self.reason = reason

        self.start_datetime = start_datetime
        self.with_lvs = with_lvs
        self.wait_for_green = wait_for_green
        self.nodes_per_run = nodes_per_run

        self.operations = operations

        if not (Operation.REBOOT in self.operations or Operation.RESTART in self.operations):
            raise ValueError('Operations should always include REBOOT or RESTART')

    @property
    def runtime_description(self):
        """Return a string that represents which operation will be performed as well as the target cluster + reason."""
        reboot_or_restart = "reboot" if Operation.REBOOT in self.operations else "restart"
        with_optional_upgrade = ("with" if Operation.UPGRADE in self.operations else "without") + " plugin upgrade"
        operation = "{} {}".format(reboot_or_restart, with_optional_upgrade)
        batch_size = "{} nodes at a time".format(self.nodes_per_run)
        return "{} ({}) for ElasticSearch cluster {}: {}".format(operation, batch_size, self.clustergroup, self.reason)

    def run(self):
        """Required by Spicerack API."""
        while True:
            if self.wait_for_green:
                self.elasticsearch_clusters.wait_for_green()

            logger.info('Fetch %d node(s) from %s to perform rolling restart on', self.nodes_per_run, self.clustergroup)
            nodes = self.elasticsearch_clusters.get_next_clusters_nodes(self.start_datetime, self.nodes_per_run)
            if nodes is None:
                break

            remote_hosts = nodes.get_remote_hosts()
            puppet = self.spicerack.puppet(remote_hosts)

            with self.spicerack.icinga_hosts(remote_hosts.hosts).downtimed(self.reason, duration=timedelta(minutes=30)):
                with puppet.disabled(self.reason):

                    with self.elasticsearch_clusters.frozen_writes(self.reason):
                        logger.info('Wait for a minimum time of 60sec to make sure all CirrusSearch writes are terminated')
                        sleep(60)

                        logger.info('Stopping elasticsearch replication in a safe way on %s', self.clustergroup)
                        with self.elasticsearch_clusters.stopped_replication():
                            self.elasticsearch_clusters.flush_markers()

                            # TODO: remove this condition when a better implementation is found.
                            if self.with_lvs:
                                nodes.depool_nodes()
                                sleep(20)

                            nodes.stop_elasticsearch()

                            self.rolling_operation(nodes)

                            nodes.wait_for_elasticsearch_up(timedelta(minutes=10))

                            # let's wait a bit to make sure everything has time to settle down
                            sleep(20)

                            # TODO: remove this condition when a better implementation is found.
                            # NOTE: we repool nodes before thawing writes and re-enabling replication since they
                            #       can already serve traffic at this point.
                            if self.with_lvs:
                                nodes.pool_nodes()

                        logger.info('wait for green on all clusters before thawing writes. If not green, still thaw writes')
                        try:
                            self.elasticsearch_clusters.wait_for_green(timedelta(minutes=5))
                        except ElasticsearchClusterCheckError:
                            logger.info('Cluster not yet green, thawing writes and resume waiting for green')
                        # TODO: inspect the back pressure on the kafka queue so that nothing
                        #       is attempted if it's too high.

            logger.info('Wait for green in %s before fetching next set of nodes', self.clustergroup)
            self.elasticsearch_clusters.wait_for_green()

            logger.info('Allow time to consume write queue')
            self.elasticsearch_clusters.wait_for_all_write_queues_empty()

    def rolling_operation(self, nodes):
        """
        Performs rolling Elasticsearch service restarts across the cluster.
        Optionally upgrade Elasticsearch plugins before proceeding to restart/reboot.
        Optionally performs a full reboot as opposed to just restarting services.
        """
        start_time = datetime.utcnow()
        logger.info("Starting rolling_operation {} on {} at time {}".format(self.operations, nodes, start_time))

        for operation in self.operations:
            if operation is Operation.UPGRADE:
                # TODO: implement a generic and robust package upgrade mechanism in spicerack
                upgrade_cmd = 'DEBIAN_FRONTEND=noninteractive apt-get {options} install {packages}'.format(
                              options='-y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"',
                              packages=' '.join(['elasticsearch-oss', 'wmf-elasticsearch-search-plugins']))
                nodes.get_remote_hosts().run_sync(upgrade_cmd)  # pylint: disable=protected-access

            if operation is Operation.REBOOT:
                nodes.get_remote_hosts().reboot(batch_size=self.nodes_per_run)
                nodes.get_remote_hosts().wait_reboot_since(start_time)

            if operation is Operation.RESTART:
                nodes.start_elasticsearch()
