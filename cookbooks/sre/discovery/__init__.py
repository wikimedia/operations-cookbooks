"""Generic DNS Discovery Operations"""
import logging
from typing import List, Iterator

import dns

from spicerack.decorators import retry
from spicerack.dnsdisc import Discovery, DiscoveryError, DiscoveryCheckError
from spicerack.remote import Remote

logger = logging.getLogger(__name__)
__title__ = __doc__

# Some IP from a subnet of each DC (to be used for EDNS lookups)
DC_IP_MAP = {
    'eqiad': '10.64.0.1',
    'codfw': '10.192.0.1',
    'esams': '10.20.0.1',
    'ulsfo': '10.128.0.1',
    'eqsin': '10.132.0.1',
}


def resolve_with_client_ip(dnsdisc: Discovery, client_ip: str, name: str) -> Iterator[dns.resolver.Answer]:
    """Generator that yields the records as resolved from a specific datacenter (via EDNS client subnet)

    Todo:
        Move this function to spicerack dns/dnsdisc.
        Yield the answer together with the resolver it came from.

    Arguments:
        dnsdisc (spicerack.dnsdisc.Discovery): a spicerack.dnsdisc.Discovery instance.
        client_ip (str): IP address to be used in EDNS client subnet.
        name (str): record name to use for the resolution.

    Yields:
        dns.resolver.Answer: the DNS response.

    Raises:
        spicerack.discovery.DiscoveryError: if unable to resolve the address.

    """
    records = [name]
    ecs_option_client_ip = dns.edns.ECSOption(client_ip)

    for nameserver, dns_resolver in dnsdisc._resolvers.items():  # pylint: disable=protected-access
        for record in records:
            record_name = dns.name.from_text('{record}.discovery.wmnet'.format(record=record))
            rdtype = dns.rdatatype.from_text('A')
            try:
                # Craft a query message
                query_msg = dns.message.make_query(record_name, rdtype)
                query_msg.use_edns(options=[ecs_option_client_ip])

                # Make the actual query
                resp = dns.query.udp(query_msg, dns_resolver.nameservers[0], port=dns_resolver.port)
                # Build an Answer instance as a Stub Resolver would
                answer = dns.resolver.Answer(record_name, rdtype, dns.rdataclass.IN, resp)
            except Exception as e:
                # This is less than ideal, but dns.query.udp can return quite a lot of exceptions, there is no
                # abstraction in dnspython 1.16.0 (2.0.0 has some) and I'm lazy.
                raise DiscoveryError(
                    'Unable to resolve {name} from {ns}'.format(name=record_name, ns=nameserver)) from e

            logger.debug('[%s] %s -> %s TTL %d', nameserver, record, answer[0].address, answer.ttl)
            yield answer


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
    for record in resolve_with_client_ip(dnsdisc, client_ip, name):
        if record[0].address != expected_address:
            logger.error("Expected IP '%s', got '%s' for record %s", expected_address, record[0].address, name)
            if not no_fail:
                failed = True

    if failed:
        raise DiscoveryCheckError('Failed to check record {name}'.format(name=name))


def wipe_recursor_cache(services: List[str], remote: Remote):
    """Wipe the cache of DNS recursors.

    Wipe the cache on resolvers to ensure they get updated quickly.

    Todo:
        Move this function to spicerack dns/dnsdisc.

    Arguments:
        services (List[str]): list of service names to wipe cache for.
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
    old_ttl = max([r.ttl for r in dnsdisc.resolve()])
    if old_ttl == new_ttl:
        logger.info('TTL already set to %d, nothing to do', new_ttl)
    else:
        dnsdisc.update_ttl(new_ttl)
    return old_ttl
