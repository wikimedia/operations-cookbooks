"""Elasticsearch Clusters Operations"""
import argparse
import logging

from datetime import timedelta
from time import sleep

from dateutil.parser import parse
from spicerack.elasticsearch_cluster import ElasticsearchClusterCheckError

__title__ = __doc__
logger = logging.getLogger(__name__)

CLUSTERGROUPS = ('search_eqiad', 'search_codfw', 'relforge', 'cloudelastic')  # Used in imports for other files


def valid_datetime_type(datetime_str):
    """Custom argparse type for user datetime values given from the command line"""
    try:
        dt = parse(datetime_str)
        if dt.tzinfo is not None and dt.tzinfo.utcoffset(dt) is not None:
            raise argparse.ArgumentTypeError('datetime should be naive (without timezone information)')
        return dt
    except ValueError:
        msg = "Error reading datetime ({0})!".format(datetime_str)
        raise argparse.ArgumentTypeError(msg)


def execute_on_clusters(elasticsearch_clusters, reason, spicerack,  # pylint: disable=too-many-arguments
                        nodes_per_run, clustergroup, start_datetime, nodes_have_lvs,
                        wait_for_green, action):
    """Executes an action on a whole cluster, taking care of alerting, puppet, etc...

    The action itself is passed as a function `action(nodes: ElasticsearchHosts)`.
    TODO: refactor this mess to reduce the number of arguments.
    """
    while True:
        if wait_for_green:
            elasticsearch_clusters.wait_for_green()

        logger.info('Fetch %d node(s) from %s to perform rolling restart on', nodes_per_run, clustergroup)
        nodes = elasticsearch_clusters.get_next_clusters_nodes(start_datetime, nodes_per_run)
        if nodes is None:
            break

        remote_hosts = nodes.get_remote_hosts()
        puppet = spicerack.puppet(remote_hosts)

        with spicerack.icinga_hosts(remote_hosts.hosts).downtimed(reason, duration=timedelta(minutes=30)):
            with puppet.disabled(reason):

                with elasticsearch_clusters.frozen_writes(reason):
                    logger.info('Wait for a minimum time of 60sec to make sure all CirrusSearch writes are terminated')
                    sleep(60)

                    logger.info('Stopping elasticsearch replication in a safe way on %s', clustergroup)
                    with elasticsearch_clusters.stopped_replication():
                        elasticsearch_clusters.flush_markers()

                        # TODO: remove this condition when a better implementation is found.
                        if nodes_have_lvs:
                            nodes.depool_nodes()
                            sleep(20)

                        action(nodes)

                        nodes.wait_for_elasticsearch_up(timedelta(minutes=10))

                        # let's wait a bit to make sure everything has time to settle down
                        sleep(20)

                        # TODO: remove this condition when a better implementation is found.
                        # NOTE: we repool nodes before thawing writes and re-enabling replication since they
                        #       can already serve traffic at this point.
                        if nodes_have_lvs:
                            nodes.pool_nodes()

                    logger.info('wait for green on all clusters before thawing writes. If not green, still thaw writes')
                    try:
                        elasticsearch_clusters.wait_for_green(timedelta(minutes=5))
                    except ElasticsearchClusterCheckError:
                        logger.info('Cluster not yet green, thawing writes and resume waiting for green')
                    # TODO: inspect the back pressure on the kafka queue so that nothing
                    #       is attempted if it's too high.

        logger.info('Wait for green in %s before fetching next set of nodes', clustergroup)
        elasticsearch_clusters.wait_for_green()

        logger.info('Allow time to consume write queue')
        elasticsearch_clusters.wait_for_all_write_queues_empty()
