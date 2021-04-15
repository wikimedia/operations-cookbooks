"""WDQS Cookbooks"""
__title__ = __doc__

from datetime import timedelta
from spicerack.decorators import retry


def check_host_is_wdqs(remote_hosts, remote):
    """Remote hosts must be a wdqs host"""
    all_wdqs = remote.query("A:wdqs-all")
    if remote_hosts.hosts not in all_wdqs.hosts:
        raise ValueError("Selected hosts ({hosts}) must be WDQS hosts".format(hosts=remote_hosts.hosts))


@retry(tries=1000, delay=timedelta(minutes=10), backoff_mode='constant', exceptions=(ValueError,))
def wait_for_updater(prometheus, site, remote_host):
    """Wait for wdqs updater to catch up on updates.

    This might take a while to complete and is completely normal.
    Hence, the long wait time.
    """
    host = remote_host.hosts[0].split(".")[0]
    query = "scalar(time() - blazegraph_lastupdated{instance='%s:9193'})" % host
    result = prometheus.query(query, site)
    last_updated = int(result[1])
    if last_updated > 1200:
        raise ValueError("Let's wait for updater to catch up (last_updated of {} is too high)".format(last_updated))
