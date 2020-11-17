"""sre.hosts.decommission tests."""
from unittest import mock

from wmflib.dns import DnsNotFound

from cookbooks.sre.hosts.decommission import get_grep_patterns


@mock.patch('wmflib.dns.Dns', spec_set=True)
def test_get_grep_patterns_match(dns):
    """Given some DNS name it should generate the matching patterns for both DNS records and IPs."""
    dns.resolve_ips.return_value = ['10.20.30.40', 'fe80::4790:f674:dead:beef']
    patterns = get_grep_patterns(dns, ['foo.bar.tld', 'bar.foo.tld'])
    assert patterns == [
        r'foo\.bar\.tld',
        r'[^0-9A-Za-z]10\.20\.30\.40[^0-9A-Za-z]',
        r'[^0-9A-Za-z]fe80::4790:f674:dead:beef[^0-9A-Za-z]',
        r'bar\.foo\.tld',
        r'[^0-9A-Za-z]10\.20\.30\.40[^0-9A-Za-z]',
        r'[^0-9A-Za-z]fe80::4790:f674:dead:beef[^0-9A-Za-z]',
    ]


@mock.patch('wmflib.dns.Dns', spec_set=True)
def test_get_grep_patterns_no_dns(dns):
    """If the DNS records don't have any match, it should generate the patterns for the names only."""
    dns.resolve_ips.side_effect = DnsNotFound
    patterns = get_grep_patterns(dns, ['foo.bar.tld', 'bar.foo.tld'])
    assert patterns == [
        r'foo\.bar\.tld',
        r'bar\.foo\.tld',
    ]
