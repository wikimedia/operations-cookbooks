"""Mysql cookbooks."""

__owner_team__ = "Data Persistence"

import logging

from spicerack.mysql import MysqlRemoteHosts

log = logging.getLogger(__name__)


def ensure(condition: bool, msg: str) -> None:
    """Just some syntactic sugar for readability."""
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def get_mysqlremotehosts(spicerack, fqdn: str) -> MysqlRemoteHosts:
    """Returns a `MysqlRemoteHosts` instance for a single, non multiinstance host or raises if not found"""
    query = "P{" + fqdn + "} and A:db-all and not A:db-multiinstance"
    mrhs: MysqlRemoteHosts = spicerack.mysql().get_dbs(query)
    ensure(len(mrhs) == 1, f"{len(mrhs)} Mysql instances found, expected one")
    return mrhs
