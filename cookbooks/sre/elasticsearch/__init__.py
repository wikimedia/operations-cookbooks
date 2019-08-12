"""Elasticsearch Clusters Operations"""
import argparse
import logging

from datetime import datetime, timedelta
from time import sleep

from dateutil.parser import parse
from spicerack.elasticsearch_cluster import ElasticsearchClusterCheckError

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name

CLUSTERGROUPS = ('search_eqiad', 'search_codfw', 'relforge')


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


def argument_parser_base(name, title):
    """Parse the command line arguments for all the sre.elasticsearch cookbooks.

    Todo:
        Remove ``--without-lvs`` for a better implementation as this was introduced because
        relforge cluster does not have lvs enabled.

    """
    parser = argparse.ArgumentParser(prog=name, description=title,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
    parser.add_argument('admin_reason', help='Administrative Reason')
    parser.add_argument('--start-datetime', type=valid_datetime_type,
                        help='start datetime in ISO 8601 format e.g 2018-09-15T15:53:00')
    parser.add_argument('--task-id', help='task_id for the change')
    parser.add_argument('--nodes-per-run', default=3, type=int, help='Number of nodes per run.')
    parser.add_argument('--without-lvs', action='store_false', dest='with_lvs', help='This cluster does not use LVS.')
    parser.add_argument('--no-wait-for-green', action='store_false', dest='wait_for_green',
                        help='Don\'t wait for green before starting the operation (still wait at the end).')

    return parser


def post_process_args(args):
    """Do any post-processing of the parsed arguments."""
    if args.start_datetime is None:
        args.start_datetime = datetime.utcnow()


def execute_on_clusters(elasticsearch_clusters, icinga, reason, spicerack,  # pylint: disable=too-many-arguments
                        nodes_per_run, clustergroup, start_datetime, nodes_have_lvs, wait_for_green, action):
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

        with icinga.hosts_downtimed(remote_hosts.hosts, reason, duration=timedelta(minutes=30)):
            with puppet.disabled(reason):

                # TODO: remove this condition when a better implementation is found.
                if nodes_have_lvs:
                    nodes.depool_nodes()

                with elasticsearch_clusters.frozen_writes(reason):
                    logger.info('Wait for a minimum time of 60sec to make sure all CirrusSearch writes are terminated')
                    sleep(60)

                    logger.info('Stopping elasticsearch replication in a safe way on %s', clustergroup)
                    with elasticsearch_clusters.stopped_replication():
                        elasticsearch_clusters.flush_markers()

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
                    # TODO: inspect the back pressure on the kafka queue and so that nothing
                    #       is attempted if it's too high.

        logger.info('Allow time to consume write queue')
        sleep(timedelta(minutes=5).seconds)
        logger.info('Wait for green in %s before fetching next set of nodes', clustergroup)
        elasticsearch_clusters.wait_for_green()
