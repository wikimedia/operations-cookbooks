"""Depool a DNS discovery record and lower the TTL.

- Lower the TTL of the record (to not have to wait so long on repool)
- Depool the datacenter
- Wipe the DNS recursor caches for the dnsdisc record
- Wait for the old TTL to have resolvers updates

See:
    https://wikitech.wikimedia.org/wiki/DNS/Discovery

Usage example:
    cookbook sre.discovery.depool helm-charts codfw

"""
from cookbooks.sre.discovery import argument_parser_base, run_base

__title__ = 'Depool a DNS discovery service'
DEFAULT_TTL = 10  # Lower the TTL to this by default


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__title__, __doc__, default_ttl=DEFAULT_TTL)


def run(args, spicerack):
    """Lower the TTL and depool the DNS discovery record"""
    return run_base(args, spicerack, depool=True)
