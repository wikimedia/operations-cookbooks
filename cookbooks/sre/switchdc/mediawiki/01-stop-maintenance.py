"""Stop MediaWiki maintenance jobs"""
import datetime
import logging

from kubernetes import client
from spicerack.decorators import retry
from spicerack.exceptions import SpicerackCheckError

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)
NAMESPACE = 'mw-script'


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    datacenters = [args.dc_from]
    if args.live_test:
        logger.info("Skipping disable of maintenance jobs in %s (active DC)", args.dc_to)
    else:
        datacenters.append(args.dc_to)
    logger.info('Stopping MediaWiki maintenance jobs in %s', ', '.join(datacenters))
    for datacenter in datacenters:
        spicerack.mediawiki().stop_periodic_jobs(datacenter)
        batch_api = spicerack.kubernetes('main', datacenter).api.batch()
        if spicerack.dry_run:
            logger.info('Skipping deletion of mw-script Kubernetes jobs in %s, due to --dry-run', datacenter)
        else:
            # Setting a propagation policy cleans up the jobs' child pods, not just the jobs. Choosing Foreground
            # instead of Background means the jobs won't be fully deleted until the pods are already deleted. That way,
            # when _wait_for_jobs_to_stop returns, we'll know everything's gone.
            batch_api.delete_collection_namespaced_job(
                NAMESPACE, body=client.V1DeleteOptions(propagation_policy='Foreground'))
        _wait_for_jobs_to_stop(batch_api, dry_run=spicerack.dry_run)


@retry(tries=60, delay=datetime.timedelta(seconds=5), backoff_mode='constant')
def _wait_for_jobs_to_stop(batch_api: client.BatchV1Api, *, dry_run: bool) -> None:
    # This could be implemented with a Watch. It uses @retry instead because the code is simpler to read and looks more
    # like the rest of the cookbooks codebase. We also get the correct dry run behavior (one check but no retries) for
    # free; @retry reads the otherwise-unused dry_run kwarg. The tradeoff is a little more load on the k8s apiserver,
    # and a little more network traffic, both of which are perfectly affordable for the small amount of data here.
    _ = dry_run
    job_list = batch_api.list_namespaced_job(NAMESPACE)
    still_running = [job for job in job_list.items if not _is_stopped(job)]
    if not still_running:
        return
    jobs = 'job' if len(still_running) == 1 else 'jobs'
    names = ', '.join(job.metadata.name for job in still_running)
    raise SpicerackCheckError(f'{len(still_running)} maintenance {jobs} still running: {names}')


def _is_stopped(job: client.V1Job) -> bool:
    if job.status.conditions is None:
        return False
    return any(cond.status == 'True' and cond.type in {'Complete', 'Failed'} for cond in job.status.conditions)
