"""sre.ganeti.makevm tests."""
import pytest

from cookbooks.sre.ganeti.makevm import make_fqdn


@pytest.mark.parametrize("hostname,network,dc,expected", (
    ("test1001", "public", "eqiad", "test1001.wikimedia.org"),
    ("test1001", "private", "eqiad", "test1001.eqiad.wmnet"),
    ("test2001", "private", "codfw", "test2001.codfw.wmnet"),
    ("test2001-dev", "private", "codfw", "test2001-dev.codfw.wmnet"),
    # Misc. servers with no numerical part are OK too
    ("misc", "private", "eqiad", "misc.eqiad.wmnet"),
))
def test_make_fqdn_ok(hostname, network, dc, expected):
    assert make_fqdn(hostname, network, dc) == expected


@pytest.mark.parametrize("hostname,network,dc,message", (
    # eqiad should be 1XXX
    ("test2001", "private", "eqiad", "Hostname expected to match 1###, got 2001 instead"),
    # matches numbers not at the end
    ("test1001-dev", "private", "codfw", "Hostname expected to match 2###, got 1001 instead"),
))
def test_make_fqdn_fail(hostname, network, dc, message):
    with pytest.raises(RuntimeError, match=message):
        assert make_fqdn(hostname, network, dc)
