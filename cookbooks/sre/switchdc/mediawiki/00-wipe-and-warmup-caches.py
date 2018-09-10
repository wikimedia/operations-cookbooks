"""Wipe and warmup MediaWiki caches"""
import logging

from spicerack.interactive import ask_confirmation

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)
    remote = spicerack.remote()
    if args.live_test:
        logger.info('Inverting DC to perform the wipe and warmup in %s (passive DC)', args.dc_from)
        datacenter = args.dc_from
    else:
        datacenter = args.dc_to

    ask_confirmation('Are you sure to wipe and warmup caches in {dc}?'.format(dc=datacenter))

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

    maintenance_host = spicerack.mediawiki().get_maintenance_host(datacenter)
    maintenance_host.run_sync(memc_warmup, appserver_warmup)
