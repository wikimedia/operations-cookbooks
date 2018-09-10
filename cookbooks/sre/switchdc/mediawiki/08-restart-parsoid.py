"""Rolling restart of Parsoid"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    logger.info('Rolling restart of Parsoid in %s and %s', args.dc_from, args.dc_to)
    # Skip wtp1043 due to T196886
    remote_hosts = spicerack.remote().query('O:parsoid and not wtp1043.eqiad.wmnet')
    remote_hosts.run_sync('restart-parsoid', batch_size=1, batch_sleep=15.0)
