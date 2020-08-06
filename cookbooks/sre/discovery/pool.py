"""Pool a DNS discovery record and raise/reset the TTL.

- Raise/reset the TTL of the records
- Pool the datacenter
- Wipe the DNS recursor caches for the dnsdisc record
- Wait for the old TTL to have resolvers updates

See:
    https://wikitech.wikimedia.org/wiki/DNS/Discovery

Usage example:
    cookbook sre.discovery.pool helm-charts codfw

"""
from cookbooks.sre.discovery import argument_parser_base, run_base

__title__ = 'Pool a DNS discovery service'
DEFAULT_TTL = 300  # Reset the TTL to this by default


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__title__, __doc__, default_ttl=DEFAULT_TTL)


def run(args, spicerack):
    """Raise/reset the TTL and pool the DNS discovery record"""
    return run_base(args, spicerack, depool=False)
