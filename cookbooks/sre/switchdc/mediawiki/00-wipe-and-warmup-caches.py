"""Wipe and warmup MediaWiki caches"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    remote = spicerack.remote()
    if args.live_test:
        datacenter = args.dc_from  # Invert the DC and perform the wipe and warmup in the passive DC
    else:
        datacenter = args.dc_to

    logger.info('Restart MediaWiki memcached in %s (wipe memcache)', datacenter)
    remote.query('A:memcached-' + datacenter).run_sync('service memcached restart')

    logger.info('Restart MediaWiki HHVM in %s (wipe APC)', datacenter)
    remote.query('A:all-mw-' + datacenter).run_sync('service hhvm restart', batch_size=25)

    logger.info('Running warmup script in %s', datacenter)

    warmup_dir = '/var/lib/mediawiki-cache-warmup'
    memc_warmup = "nodejs {dir}/warmup.js {dir}/urls-cluster.txt spread appservers.svc.{dc}.wmnet".format(
        dir=warmup_dir, dc=datacenter)
    appserver_warmup = "nodejs {dir}/warmup.js {dir}/urls-server.txt clone appserver {dc}".format(
        dir=warmup_dir, dc=datacenter)

    mainteance_host = spicerack.mediawiki().get_maintenance_host(datacenter)
    mainteance_host.run_sync(memc_warmup, appserver_warmup)
