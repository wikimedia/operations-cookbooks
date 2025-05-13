"""Generic DNS Discovery Operations"""
import logging

from ipaddress import IPv4Address
from spicerack.decorators import retry
from spicerack.dnsdisc import Discovery, DiscoveryCheckError
from spicerack.remote import Remote

logger = logging.getLogger(__name__)
__owner_team__ = "ServiceOps"

# Some IP from a subnet of each DC (to be used for EDNS lookups)
DC_IP_MAP = {
    'eqiad': IPv4Address('10.64.0.1'),
    'codfw': IPv4Address('10.192.0.1'),
    'esams': IPv4Address('10.80.0.1'),
    'ulsfo': IPv4Address('10.128.0.1'),
    'eqsin': IPv4Address('10.132.0.1'),
    'drmrs': IPv4Address('10.136.0.1'),
    'magru': IPv4Address('10.140.0.1'),
}


@retry(backoff_mode='linear', exceptions=(DiscoveryCheckError,))
def check_record_for_dc(no_fail: bool, dnsdisc: Discovery, datacenter: str, name: str, expected_name: str):
    """Check that a Discovery record resolves on all authoritative resolvers to the correct IP.

    The IP to use for the comparison is obtained resolving the expected_name record.
    For example with name='servicename-rw.discovery.wmnet' and expected_name='servicename.svc.eqiad.wmnet', this
    method will resolve the 'expected_name' to get its IP address and then verify that on all authoritative
    resolvers the record for 'name' resolves to the same IP.
    It is retried to allow the change to be propagated through all authoritative resolvers.

    Todo:
        Move this function to spicerack dns/dnsdisc.

    See Also:
        https://wikitech.wikimedia.org/wiki/DNS/Discovery

    Arguments:
        no_fail (bool): don't fail is address does not match.
        dnsdisc (spicerack.dnsdisc.Discovery): a spicerack.dnsdisc.Discovery instance.
        datacenter (str): name of the datacenter used as EDNS client subnet.
        name (str): the record to resolve.
        expected_name (str): another, non-discovery dns record, known to resolve to a specific IP.

    Raises:
        DiscoveryError: if the record doesn't match the IP of the expected_name.

    """
    expected_address = dnsdisc.resolve_address(expected_name)
    logger.info('Checking that %s.discovery.wmnet records for %s matches %s (%s)',
                name, datacenter, expected_name, expected_address)

    failed = False
    client_ip = DC_IP_MAP[datacenter]
    for nameserver, actual_ip in dnsdisc.resolve_with_client_ip(name, client_ip).items():
        if str(actual_ip) != expected_address:
            logger.error("Expected IP '%s', got '%s' for record %s from %s",
                         expected_address, actual_ip, name, nameserver)
            if not no_fail:
                failed = True

    if failed:
        raise DiscoveryCheckError('Failed to check record {name}'.format(name=name))


def wipe_recursor_cache(services: list[str], remote: Remote):
    """Wipe the cache of DNS recursors.

    Wipe the cache on resolvers to ensure they get updated quickly.

    Todo:
        Move this function to spicerack dns/dnsdisc.

    Arguments:
        services (list[str]): list of service names to wipe cache for.
        remote (spicerack.remote.Remote): spicerack.remote.Remote instance.

    See Also:
        https://wikitech.wikimedia.org/wiki/DNS#How_to_Remove_a_record_from_the_DNS_resolver_caches

    """
    recursor_hosts = remote.query('A:dns-rec')
    records = ' '.join(['{record}.discovery.wmnet'.format(record=r) for r in services])
    wipe_cache_cmd = 'rec_control wipe-cache {records}'.format(records=records)
    recursor_hosts.run_async(wipe_cache_cmd)


def update_ttl(dnsdisc: Discovery, new_ttl: int) -> int:
    """Update the TTL for records in dnsdisc.

    Todo:
        Should get TTL from conftool instead of DNS but its not exposed currently
        https://phabricator.wikimedia.org/T259875
        Set the TTL for only one DC in dnsdisc.update_ttl
        Move this function to spicerack dns/dnsdisc.

    Arguments:
        dnsdisc (spicerack.dnsdisc): dnsdisc instance.
        new_ttl (int): the new TTL to set.

    """
    # Get the old TTL
    old_ttl = max(r.ttl for r in dnsdisc.resolve())
    if old_ttl == new_ttl:
        logger.info('TTL already set to %d, nothing to do', new_ttl)
    else:
        dnsdisc.update_ttl(new_ttl)
    return old_ttl
