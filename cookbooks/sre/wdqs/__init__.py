"""WDQS Cookbooks"""
__title__ = __doc__

from datetime import timedelta
from spicerack.decorators import retry

MUTATION_TOPIC = "rdf-streaming-updater.mutation"


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
    lag = float(result[1])
    if lag > 1200.0:
        raise ValueError("Let's wait for updater to catch up (lag of {} is too high)".format(lag))


def get_site(host, spicerack):
    """Get site for the host."""
    netbox_server = spicerack.netbox_server(host)
    site = netbox_server.as_dict()['site']['slug']
    return site


def get_hostname(fqdn):
    """Get short hostname from a Fully Qualified Domain Name"""
    return fqdn.split(".")[0]
