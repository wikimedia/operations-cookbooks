'''
Tests, what else?
'''

from unittest import mock
from cookbooks.sre.hosts.decommission import get_grep_patterns


@mock.patch('wmflib.dns.Dns', spec_set=True)
def test_get_grep_patterns(dns):
    '''
    Let's test this function
    '''
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
