"""WDQS Cookbooks"""
__owner_team__ = "Data Platform"

import logging
from datetime import timedelta, datetime

from spicerack import RemoteHosts, ConftoolEntity
from spicerack.confctl import ConfctlError
from spicerack.decorators import retry

MUTATION_TOPICS = {
    'wikidata': 'rdf-streaming-updater.mutation',
    'wikidata_full': 'rdf-streaming-updater.mutation',
    'wikidata_main': 'rdf-streaming-updater.mutation-main',
    'scholarly_articles': 'rdf-streaming-updater.mutation-scholarly',
    'commons': 'mediainfo-streaming-updater.mutation',
}

logger = logging.getLogger(__name__)


def check_hosts_are_valid(remote_hosts, remote):
    """Remote hosts must be exclusively wdqs or wcqs hosts"""
    all_wdqs = remote.query("A:wdqs-all")
    if remote_hosts.hosts in all_wdqs.hosts:
        return 'wdqs'
    all_wcqs = remote.query("A:wcqs-public")
    if remote_hosts.hosts in all_wcqs.hosts:
        return 'wcqs'
    raise ValueError("Selected hosts ({hosts}) must be all be query service hosts for the same dataset".format(
        hosts=remote_hosts.hosts))


@retry(tries=1000, delay=timedelta(minutes=10), backoff_mode='constant', exceptions=(ValueError,))
def wait_for_updater(prometheus, site, remote_host):
    """Wait for query service updater to catch up on updates.

    This might take a while to complete and is completely normal.
    Hence, the long wait time.
    """
    host = remote_host.hosts[0].split(".")[0]
    query = "scalar(time() - blazegraph_lastupdated{instance=~'%s:919[35]'})" % host
    result = prometheus.query(query, site)
    lag = float(result[1])
    if lag > 1200.0:
        raise ValueError("Let's wait for updater to catch up (lag of {} is too high)".format(lag))


def get_site(host, netbox):
    """Get site for the host."""
    server = netbox.get_server(host)
    return server.as_dict()['site']['slug']


def get_hostname(fqdn):
    """Get short hostname from a Fully Qualified Domain Name"""
    return fqdn.split(".")[0]


class StopWatch:
    """Stop watch to measure time."""

    def __init__(self) -> None:
        """Create a new StopWatch initialized with current time."""
        self._start_time = datetime.now()

    def elapsed(self) -> timedelta:
        """Returns the time elapsed since the StopWatch was started."""
        end_time = datetime.now()
        return end_time - self._start_time

    def reset(self):
        """Reset the StopWatch to current time."""
        self._start_time = datetime.now()


def is_behind_lvs(conftool: ConftoolEntity, remote_host: RemoteHosts) -> bool:
    """Check for LVS on host by looking for the 'pool' command"""
    if len(remote_host.hosts) > 1:
        raise ValueError("Only one host supported by this function")
    try:
        next(conftool.get(name=str(remote_host.hosts)))
        return True
    except ConfctlError:
        # ConftoolEntity raises ConfctlError("No match found") when no entries are found
        logger.info('This host is not behind LVS')
        return False
