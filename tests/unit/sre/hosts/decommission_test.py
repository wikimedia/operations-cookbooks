'''
Tests, what else?
'''

from unittest import mock
from cookbooks.sre.hosts.decommission import get_grep_patterns


@mock.patch('spicerack.dns.Dns', spec_set=True)
def test_get_grep_patterns(dns):
    '''
    Let's test this function
    '''
    dns.resolve_ips.return_value = ['10.20.30.40']
    patterns = get_grep_patterns(dns, ['foo.bar.tld', 'bar.foo.tld'])
    assert patterns == [
        r'foo\.bar\.tld',
        r'10\.20\.30\.40',
        r'bar\.foo\.tld',
        r'10\.20\.30\.40'
        ]
