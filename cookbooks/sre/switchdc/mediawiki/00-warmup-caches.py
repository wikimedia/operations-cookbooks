"""Warmup MediaWiki caches"""
import datetime
import itertools
import logging

from wmflib.interactive import ask_confirmation

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


MINIMUM_ITERATIONS = 6  # How many loops to do, at minimum
__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    if args.live_test:
        logger.info('Inverting DC to perform the warmup in %s (passive DC)', args.dc_from)
        datacenter = args.dc_from
    else:
        datacenter = args.dc_to

    ask_confirmation('Are you sure to warmup caches in {dc}?'.format(dc=datacenter))

    warmup_dir = '/var/lib/mediawiki-cache-warmup'
    # urls-cluster is only running against appservers since is for shared resources behind the
    # servers themselves
    warmups = ["nodejs {dir}/warmup.js {dir}/urls-cluster.txt spread appservers.svc.{dc}.wmnet".format(
        dir=warmup_dir, dc=datacenter)]
    for cluster in ["appserver", "api_appserver"]:
        # urls-server runs against both appserver and API clusters since it's for each individual server
        warmups.append("nodejs {dir}/warmup.js {dir}/urls-server.txt clone {cluster} {dc}".format(
            dir=warmup_dir, dc=datacenter, cluster=cluster))

    maintenance_host = spicerack.mediawiki().get_maintenance_host(datacenter)
    # It takes multiple executions of the warmup script to fully warm up the appserver caches. The second run is faster
    # than the first, and so on. Empirically, we consider the caches to be fully warmed up when this speedup disappears;
    # that is, when the execution time converges, and each attempt takes about as long as the one before.
    logger.info('Running warmup script in %s.', datacenter)
    logger.info('The script will re-run until execution time converges.')
    last_duration = datetime.timedelta.max
    for i in itertools.count(1):
        logger.info('Running warmup script, take %d', i)
        start_time = datetime.datetime.utcnow()
        maintenance_host.run_sync(*warmups)
        duration = datetime.datetime.utcnow() - start_time
        logger.info('Warmup completed in %s', duration)
        # After we've done a minimum number of iterations, we stop looping as soon as the warmup script takes more
        # than 95% as long as the previous run. That is, keep looping as long as it keeps going faster than before,
        # but with a 5% margin of error. At that point, any further reduction is probably just noise.
        if i >= MINIMUM_ITERATIONS and duration > 0.95 * last_duration:
            break
        last_duration = duration
    logger.info('Execution time converged, warmup complete.')
