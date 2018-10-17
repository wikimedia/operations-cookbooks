"""Rolling restart of elasticsearch service"""
import logging

from datetime import timedelta
from time import sleep

from cookbooks.sre.elasticsearch import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    icinga = spicerack.icinga()
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup)
    reason = spicerack.admin_reason(args.admin_reason, task_id=args.task_id)

    while True:
        elasticsearch_clusters.wait_for_green()

        logger.info('Fetch %d nodes from %s to perform rolling restart on', args.nodes_per_run, args.clustergroup)
        next_nodes = elasticsearch_clusters.get_next_clusters_nodes(args.start_datetime, args.nodes_per_run)
        if next_nodes is None:
            break

        remote_hosts = next_nodes.get_remote_hosts()

        icinga.downtime_hosts(remote_hosts.hosts, reason, duration=timedelta(minutes=30))

        puppet = spicerack.puppet(remote_hosts)
        with puppet.disabled(reason):
            with elasticsearch_clusters.frozen_writes(reason):
                logger.info('Wait for a minimum time of 60sec to make sure all CirrusSearch writes are terminated')
                sleep(60)

                logger.info('Stopping elasticsearch replication in a safe way on %s', args.clustergroup)
                with elasticsearch_clusters.stopped_replication():
                    elasticsearch_clusters.flush_markers()

                    next_nodes.depool_nodes()

                    next_nodes.stop_elasticsearch()
                    next_nodes.start_elasticsearch()

                    next_nodes.wait_for_elasticsearch_up(timedelta(minutes=10))

                    next_nodes.pool_nodes()

                logger.info('wait for green on all clusters before thawing writes. If not green, still thaw writes')
                elasticsearch_clusters.wait_for_green(timedelta(minutes=5))
                # TODO: inspect the back pressure on the kafka queue and so that nothing is attempted if it's too high.

        logger.info('Wait for green in %s before fetching next set of nodes', args.clustergroup)
        elasticsearch_clusters.wait_for_green()
