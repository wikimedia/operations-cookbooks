"""Rolling upgrade of elasticsearch service"""
import logging

from cookbooks.sre.elasticsearch import argument_parser_base, post_process_args, execute_on_clusters

__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    icinga = spicerack.icinga()
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup, args.write_queue_datacenters)
    reason = spicerack.admin_reason(args.admin_reason, task_id=args.task_id)

    def upgrade_elasticsearch(nodes):
        nodes.stop_elasticsearch()
        command = 'DEBIAN_FRONTEND=noninteractive apt-get {options} install {packages}'.format(
            options='-y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold"',
            packages=' '.join(['elasticsearch-oss', 'wmf-elasticsearch-search-plugins']))
        # TODO: implement a generic and robust package upgrade mechanism in spicerack
        nodes._remote_hosts.run_sync(command)  # pylint: disable=protected-access
        nodes.start_elasticsearch()

    execute_on_clusters(
        elasticsearch_clusters, icinga, reason, spicerack, args.nodes_per_run,
        args.clustergroup, args.start_datetime, args.with_lvs, args.wait_for_green, upgrade_elasticsearch
    )
