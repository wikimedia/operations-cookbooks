"""WMCS Wikireplicas add wiki cookbook

Usage example:
    cookbook wmcs.wikireplicas.add_wiki --task-id T12345 zhuwikisource

"""
import argparse
import logging

__title__ = "WMCS wikireplicas wiki adding cookbook"
logger = logging.getLogger(__name__)


def argument_parser():
    """Parse the command line arguments for this cookbook."""
    parser = argparse.ArgumentParser(
        prog=__name__,
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--task-id", required=True, help="task_id for the change"
    )
    parser.add_argument(
        "database",
        metavar="<database_name>",
        type=str,
        help="Name of the wiki DB",
    )

    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    remote = spicerack.remote()
    replicas = remote.query("A:wikireplicas-all")
    # Get a cloudcontrol host to run the DNS update on
    cloudcontrol = remote.query("A:cloudcontrol")
    control_host = next(cloudcontrol.split(len(cloudcontrol)))

    index_cmd = (
        f"/usr/local/sbin/maintain-replica-indexes --database {args.database}"
    )
    view_cmd = f"/usr/local/sbin/maintain-views --databases {args.database}"
    meta_p_cmd = f"/usr/local/sbin/maintain-meta_p --databases {args.database}"
    wiki_dns_cmd = "source /root/novaenv.sh; wmcs-wikireplica-dns --aliases"
    logger.info("Generating views...")
    replicas.run_async(index_cmd, view_cmd)
    logger.info("Adding DNS")
    control_host.run_sync(wiki_dns_cmd)
    logger.info("Finalizing meta_p")
    replicas.run_async(meta_p_cmd)
    spicerack.irc_logger.info(
        "Added views for new wiki: %s %s", args.database, args.task_id
    )
