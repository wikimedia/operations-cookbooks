"""Wipe and warmup MediaWiki caches"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    remote = spicerack.remote()

    logger.info('Restart MediaWiki memcached in %s (wipe memcache)', args.dc_to)
    remote.query('A:memcached-' + args.dc_to).run_sync('service memcached restart')

    logger.info('Restart MediaWiki HHVM in %s (wipe APC)', args.dc_to)
    remote.query('A:all-mw-' + args.dc_to).run_sync('service hhvm restart', batch_size=25)

    logger.info('Running warmup script in %s', args.dc_to)

    warmup_dir = '/var/lib/mediawiki-cache-warmup'
    memc_warmup = "nodejs {dir}/warmup.js {dir}/urls-cluster.txt spread appservers.svc.{dc}.wmnet".format(
        dir=warmup_dir, dc=args.dc_to)
    appserver_warmup = "nodejs {dir}/warmup.js {dir}/urls-server.txt clone appserver {dc}".format(
        dir=warmup_dir, dc=args.dc_to)

    mainteance_host = spicerack.mediawiki().get_maintenance_host(args.dc_to)
    mainteance_host.sync(memc_warmup, appserver_warmup)
