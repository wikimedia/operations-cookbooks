"""Warmup MediaWiki caches"""
import datetime
import itertools
import logging

from wmflib.interactive import ask_confirmation

from cookbooks.sre.hosts import DEPLOYMENT_HOST
from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args, MEDIAWIKI_RO_SERVICES


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

    current_ro_state = spicerack.discovery(*MEDIAWIKI_RO_SERVICES).active_datacenters
    datacenter_pooled = any(datacenter in current_ro_state.get(svc, {}) for svc in MEDIAWIKI_RO_SERVICES)
    if datacenter_pooled:
        ask_confirmation(f'{datacenter} is pooled for read-only traffic, you '
                         'should NOT need warming up of caches. Do you still want to proceed?')
    else:
        ask_confirmation(f'Are you sure to warmup caches in {datacenter}?')

    warmup_dir = '/var/lib/mediawiki-cache-warmup'
    # urls-cluster.txt contains requests that are useful for warming up shared resources (e.g., memcache), so it only
    # needs run against a single service. urls-server.txt, in contrast, contains requests for warming local resources
    # (e.g., APCu), and is thus run against mw-web and both API services.
    warmups = [f"{warmup_dir}/warmup.py {warmup_dir}/urls-cluster.txt spread mw-web.svc.{datacenter}.wmnet:4450"]
    warmups.extend(
        f"{warmup_dir}/warmup.py {warmup_dir}/urls-server.txt clone {datacenter} {namespace}"
        for namespace in ["mw-web", "mw-api-ext", "mw-api-int"]
    )

    deployment_host = spicerack.remote().query(spicerack.dns().resolve_cname(DEPLOYMENT_HOST))
    # It takes multiple executions of the warmup script to fully warm up the caches. The second run is faster than the
    # first, and so on. Empirically, we consider the caches to be fully warmed up when this speedup disappears; that is,
    # when the execution time converges, and each attempt takes about as long as the one before.
    logger.info('Running warmup script in %s.', datacenter)
    logger.info('The script will re-run until execution time converges.')
    last_duration = datetime.timedelta.max
    for i in itertools.count(1):
        logger.info('Running warmup script, take %d', i)
        start_time = datetime.datetime.utcnow()
        deployment_host.run_sync(*warmups)
        duration = datetime.datetime.utcnow() - start_time
        logger.info('Warmup completed in %s', duration)
        # After we've done a minimum number of iterations, we stop looping as soon as the warmup script takes more
        # than 95% as long as the previous run. That is, keep looping as long as it keeps going faster than before,
        # but with a 5% margin of error. At that point, any further reduction is probably just noise.
        if i >= MINIMUM_ITERATIONS and duration > 0.95 * last_duration:
            break
        last_duration = duration
    logger.info('Execution time converged, warmup complete.')
