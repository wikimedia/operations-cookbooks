"""Swift Clusters Operations"""
__owner_team__ = "Data Persistence"

import re

from wmflib.dns import Dns
from spicerack.remote import Remote, RemoteHosts


def find_db_paths(dns: Dns, remote: Remote,
                  host: str, container: str) -> list[tuple[str, str]]:
    """Query the container ring on host about container

    return a list of (fqdn,path) tuples for the locations where the
    container DBs can be found.
    """
    dbregex = re.compile(
        r'^ssh (?P<ip>([0-9]{1,3}\.){3}[0-9]{1,3}) "ls.*\}(?P<path>/[^ ]+)"$',
        re.MULTILINE)
    rh = remote.query(f"D{{{host}}}")
    if len(rh.hosts) > 1:
        raise ValueError("Should only query 1 host for db paths")
    results = rh.run_sync(f"/usr/bin/swift-get-nodes /etc/swift/container.ring.gz AUTH_mw {container}",
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
