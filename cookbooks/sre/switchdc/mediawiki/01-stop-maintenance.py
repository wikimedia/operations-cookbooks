"""Stop MediaWiki maintenance and cron jobs."""

import datetime
import logging

from kubernetes import client
from spicerack.decorators import retry
from spicerack.exceptions import SpicerackCheckError

from cookbooks.sre.switchdc.mediawiki import MediaWikiSwitchDCBase, MediaWikiSwitchDCRunnerBase

logger = logging.getLogger(__name__)
NAMESPACES = ['mw-script', 'mw-cron']


class StopMaintenanceJobsRunner(MediaWikiSwitchDCRunnerBase):
    """A runner to stop MediaWiki maintenance jobs."""

    def action(self):
        """Required by base class API."""
        datacenters = [self.dc_from]
        if self.live_test:
            logger.info("Skipping disable of maintenance jobs in %s (active DC)", self.dc_to)
        else:
            datacenters.append(self.dc_to)
        logger.info('Stopping MediaWiki maintenance jobs in %s', ', '.join(datacenters))
        for datacenter in datacenters:
            self.spicerack.mediawiki().stop_periodic_jobs(datacenter)
            batch_api = self.spicerack.kubernetes('main', datacenter).api.batch()
            if self.spicerack.dry_run:
                logger.info('Skipping deletion of %s Kubernetes jobs in %s, due to --dry-run',
                            ",".join(NAMESPACES), datacenter)
            else:
                # Setting a propagation policy cleans up the jobs' child pods and the cronjobs' pods/jobs,
                # not just the jobs. Choosing Foreground instead of Background means the jobs won't be fully
                # deleted until the pods are already deleted. That way, when _wait_for_jobs_to_stop returns,
                # we'll know everything's gone.
                batch_api.delete_collection_namespaced_job(
                    'mw-script', body=client.V1DeleteOptions(propagation_policy='Foreground'))
                batch_api.delete_collection_namespaced_cron_job(
                    'mw-cron', body=client.V1DeleteOptions(propagation_policy='Foreground'))
            for namespace in NAMESPACES:
                _wait_for_jobs_to_stop(batch_api, namespace=namespace, dry_run=self.spicerack.dry_run)


@retry(tries=60, delay=datetime.timedelta(seconds=5), backoff_mode='constant')
def _wait_for_jobs_to_stop(batch_api: client.BatchV1Api, *, namespace: str, dry_run: bool) -> None:
    # This could be implemented with a Watch. It uses @retry instead because the code is simpler to read and looks more
    # like the rest of the cookbooks codebase. We also get the correct dry run behavior (one check but no retries) for
    # free; @retry reads the otherwise-unused dry_run kwarg. The tradeoff is a little more load on the k8s apiserver,
    # and a little more network traffic, both of which are perfectly affordable for the small amount of data here.
    _ = dry_run
    job_list = batch_api.list_namespaced_job(namespace)
    still_running = [job for job in job_list.items if not _is_stopped(job)]
    if not still_running:
        return
    jobs = 'job' if len(still_running) == 1 else 'jobs'
    names = ', '.join(job.metadata.name for job in still_running)
    raise SpicerackCheckError(f'{len(still_running)} {namespace} {jobs} still running: {names}')


def _is_stopped(job: client.V1Job) -> bool:
    if job.status.conditions is None:
        return False
    return any(cond.status == 'True' and cond.type in {'Complete', 'Failed'} for cond in job.status.conditions)


class StopMaintenanceJobs(MediaWikiSwitchDCBase):
    """Stop MediaWiki maintenance and periodic jobs."""

    runner_class = StopMaintenanceJobsRunner
