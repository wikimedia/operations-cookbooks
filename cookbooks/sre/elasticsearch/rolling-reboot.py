"""Rolling reboot of elasticsearch servers"""
import logging
from datetime import datetime

from cookbooks.sre.elasticsearch import argument_parser_base, post_process_args, execute_on_clusters

__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    icinga = spicerack.icinga()
    elasticsearch_clusters = spicerack.elasticsearch_clusters(args.clustergroup, args.write_queue_datacenters)
    reason = spicerack.admin_reason(args.admin_reason, task_id=args.task_id)

    def reboot(nodes):
        reboot_time = datetime.utcnow()
        nodes.get_remote_hosts().reboot(batch_size=3)
        nodes.get_remote_hosts().wait_reboot_since(reboot_time)

    execute_on_clusters(
        elasticsearch_clusters, icinga, reason, spicerack, args.nodes_per_run,
        args.clustergroup, args.start_datetime, args.with_lvs, args.wait_for_green,
        reboot
    )
