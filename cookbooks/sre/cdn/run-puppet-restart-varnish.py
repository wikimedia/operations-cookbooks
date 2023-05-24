"""Swap port 80 from Varnish to HAProxy, stopping Varnish first and then running Puppet.

This cookbook is useful when managing both Varnish and HAProxy configuration
and a ordered restart/reload is needed.
Has been created to swap the daemon listening on port 80 from Varnish to HAProxy
but can be used as template for other tasks too.

Those are the steps provided by the cookbook:

- Depool the host (automatic)
- Disable icinga notification (automatic)
- Check that puppet-agent is disabled (pre_scripts)
- Stop Varnish service (_custom_action)
- Enable and run Puppet (puppet agent **must** be disabled first with cumin) (_custom_action())
- Start Varnish service (_custom_action())
- Test that ports (80 and 443) are open (post_action)
- Repool the host (automatic)
"""

from argparse import Namespace

import requests

from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure

from spicerack import Spicerack
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


def check_http_redirect(host):
    """Check if HAProxy handle redirect correctly.

    Check also Varnish does not listen on port 80 (Varnish uses 'TLS Redirect'
    reason).

    Returns:
        bool: False if any of the requirement fails.

    """
    headers = {'Host': 'wikimedia.org'}

    s = "http://{}".format(host)
    res = requests.head(s, headers=headers, timeout=2)
    if not res.is_redirect:
        return False
    if res.reason != 'Moved Permanently':
        return False
    return True


class MovePort80(SREBatchBase):
    r"""Roll apply configuration using puppet-agent for both HAProxy and Varnish

    This cookbook is useful when managing both Varnish and HAProxy configuration
    and a ordered restart/reload is needed.
    Has been created to swap the daemon listening on port 80 from Varnish to HAProxy
    but can be used as template for other tasks too.

    Example usage(s):

        cookbook.sre.cdn.run-puppet-restart-varnish \
            --alias cp-text_codfw \
            --reason 'Let HAProxy manage port 80' \
            --puppet-reason 'TXXXXXX' \
            --grace-sleep 1200
    """

    grace_sleep = 1200  # Wait 20m between batches
    batch_max = 1
    valid_actions = ('custom',)
    max_failed = 1

    def argument_parser(self):
        """Argument parser"""
        parser = super().argument_parser()
        parser.add_argument(
            '--puppet-reason',
            type=str,
            required=True,
            help="Puppet reason. Must match puppet disable reason used by Cumin"
        )
        return parser

    # required
    def get_runner(self, args):
        """As specified by the Spicerack API."""
        return MovePort80Runner(args, self.spicerack)


class MovePort80Runner(SRELBBatchRunnerBase):
    r"""Roll apply configuration using puppet-agent for both HAProxy and Varnish."""

    depool_sleep = 30
    repool_sleep = 30

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """We need to override this in order to use the args.puppet_reason"""
        super().__init__(args, spicerack)
        self._puppet_reason = spicerack.admin_reason(reason=args.puppet_reason)

    @property
    def allowed_aliases(self):
        """Required"""
        aliases = []
        for role in ('text', 'upload'):
            for dc in ALL_DATACENTERS:
                aliases.append(f'cp-{role}_{dc}')
        return aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Override the parent property to optimize the query."""
        return 'A:cp'  # This query must include all hosts matching all the allowed_aliases

    def _custom_action(self, hosts):
        """The actual stop varnish / run puppet / start varnish logic resides here."""
        confirm_on_failure(hosts.run_async,
                           '/usr/bin/systemctl stop varnish-frontend.service')
        puppet = self._spicerack.puppet(hosts)
        confirm_on_failure(puppet.run, enable_reason=self._puppet_reason)
        confirm_on_failure(hosts.run_async,
                           '/usr/bin/systemctl start varnish-frontend.service')

    @property
    def pre_scripts(self):
        """We check that puppet agent is NOT enabled and fail in case by running shell command on the host."""
        return [
            '! puppet-enabled'
        ]

    def post_action(self, hosts):
        """Just to check that ports passed as parameters are open."""
        if not check_http_redirect(hosts.hosts):
            raise RuntimeError(f"Redirect check on host {hosts.hosts} failed!")
        super().post_action(hosts)
