"""Invert the replication flow for Redis sessions"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    redis = spicerack.redis_cluster('sessions')

    logger.info('Stopping replication in %s for the sessions Redis cluster', args.dc_to)
    redis.stop_replica(args.dc_to)

    logger.info('Starting replication %s => %s for the sessions Redis cluster', args.dc_to, args.dc_from)
    redis.start_replica(args.dc_from, args.dc_to)
