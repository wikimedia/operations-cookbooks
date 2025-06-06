"""Class based cookbook to wipe dns cache entries"""
import shlex

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.constants import ALL_DATACENTERS


class WipeCache(CookbookBase):
    """Cookbook to wipedns cache entries.

    Perform the actions outlined in the following wiki:
    https://wikitech.wikimedia.org/wiki/DNS#Wipe_caches

    Usage example:
        cookbook sre.dns.wipe-cache puppet.esqin.wmnet
        cookbook sre.dns.wipe-cache --site esqin puppet.esqin.wmnet
        cookbook sre.dns.wipe-cache ulsfo.wmnet$
        cookbook sre.dns.wipe-cache host1001.eqiad.wmnet host2001.codfw.wmnet
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            '-s',
            '--site',
            choices=ALL_DATACENTERS,
            help=(
                'This is used to limit the recursors that will be wiped. '
                'Passing eqiad here will only wipe dns entries from the eqiad servers'
            ),
        )
        parser.add_argument(
            'domains',
            nargs='+',
            help=('The DNS domains to wipe from the cache. Multiple domains can be passed. '
                  'The domains can be suffixed with a ‘$’. to delete the whole tree from the cache.'),
        )
        return parser

    def get_runner(self, args):
        """Required by Spicerack API."""
        return WipeCacheRunner(args, self.spicerack)


class WipeCacheRunner(CookbookRunnerBase):
    """Wipe a DNS cache entry."""

    _dns_rec_alias = 'A:dnsbox'

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.wipe_command = ' '.join([shlex.quote(part) for part in args.domains])
        if args.site:
            query = f'{self._dns_rec_alias} and A:{args.site}'
            self.message = f'{self.wipe_command} on {args.site} recursors'
        else:
            query = self._dns_rec_alias
            self.message = f'{self.wipe_command} on all recursors'
        self.remote_hosts = spicerack.remote().query(query)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the downtime action."""
        return self.message

    def run(self):
        """Required by Spicerack API."""
        command = f'sudo rec_control wipe-cache {self.wipe_command}'
        self.remote_hosts.run_sync(command)
