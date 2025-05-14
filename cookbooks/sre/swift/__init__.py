"""Swift Clusters Operations"""
__owner_team__ = "Data Persistence"

import re
from typing import Optional

from wmflib.dns import Dns
from spicerack.remote import Remote, RemoteHosts


def find_db_paths(dns: Dns, host: RemoteHosts,
                  container: str) -> list[tuple[str, str]]:
    """Query the container ring on host about container

    return a list of (fqdn,path) tuples for the locations where the
    container DBs can be found.
    """
    dbregex = re.compile(
        r'^ssh (?P<ip>([0-9]{1,3}\.){3}[0-9]{1,3}) "ls.*\}(?P<path>/[^ ]+)"$',
        re.MULTILINE)
    if len(host) > 1:
        raise ValueError("Should only query 1 host for db paths")
    results = host.run_sync(f"/usr/bin/swift-get-nodes /etc/swift/container.ring.gz AUTH_mw {container}",
                            is_safe=True,
                            print_output=False,
                            print_progress_bars=False)
    res = RemoteHosts.results_to_list(results)[0][1]
    ans = []
    for m in dbregex.finditer(res):
        ip = m.group('ip')
        fqdn = dns.resolve_ptr(ip)[0]
        dirpath = m.group('path')
        # dirpath is /sdXX/containers/a/b/c
        # db file is in /srv/swift-storage/dirpath/c.db
        # we ignore the .db.pending file
        dbname = f"{dirpath.split('/')[-1]}.db"
        ans.append((fqdn, f"/srv/swift-storage{dirpath}/{dbname}"))
    return ans


def lookup_be_host(remote: Remote, dc: Optional[str],
                   hostname: Optional[str]) -> RemoteHosts:
    """Return RemoteHosts of hostname if non-None, a backend host in dc"""
    query = f"A:swift-be-{dc}" if dc is not None else "A:swift-be"
    # will raise spicerack.remote.RemoteError if no matching host(s) found
    if hostname is not None:
        query += f" and P{{{hostname}.*}}"
    backends = remote.query(query)

    # Split out 1 matching hostname
    return next(iter(backends))


def lookup_fe_host(remote: Remote, dc: Optional[str],
                   hostname: Optional[str]) -> str:
    """Return fqdn(hostname) if non-None, else fqdn of a frontend host

    If dc is specified, the frontend will be in that dc. The host
    returned will be a stats_reporter host; if hostname is specified
    it must be a stats_reporter host and in the correct dc.

    """
    query = f"A:swift-fe-{dc}" if dc is not None else "A:swift-fe"
    query += " and P{C:swift::stats_reporter%ensure = present}"
    if hostname is not None:
        query += f" and P{{{hostname}.*}}"
    frontends = remote.query(query)

    # take 1 frontend, convert to string
    return str(next(iter(frontends)))
