"""Rolling service restart of pdns-recursor or HAProxy on A:dnsbox"""

from spicerack import Spicerack
from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class DNSBoxRollRestart(SREBatchBase):
    """Rolling restart of pdns-recursor or HAProxy on A:dnsbox.

    By default, it will restart both pdns-recursor and haproxy, unless a
    service is passed through --service.

    Example usage:
        cookbook sre.dns.roll-restart \
            --query 'A:dnsbox and not P{dns1004*}' \
            --reason "Scheduled maintenance" \
            restart_daemons

        cookbook sre.dns.roll-restart \
            --query 'A:dnsbox and not P{dns1004*}' \
            --reason "Scheduled maintenance" \
            --service pdns-recursor \
            restart_daemons
    """

    batch_max = 1
    batch_default = 1
    grace_sleep = 120
    min_grace_sleep = 60

    valid_actions = ("restart_daemons",)

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("--service",
                            choices=("pdns-recursor", "haproxy"),
                            help="Optional name of the service to restart")
        return parser

    def get_runner(self, args) -> SRELBBatchRunnerBase:
        """As specified by Spicerack API."""
        return DNSBoxRollRestartRunner(args, self.spicerack)


class DNSBoxRollRestartRunner(SRELBBatchRunnerBase):
    """Rolling restart of pdns-recursor or HAProxy on A:dnsbox"""

    depool_sleep = 30
    repool_sleep = 60

    def __init__(self, args, spicerack: Spicerack):
        """Initialize runner."""
        super().__init__(args, spicerack)
        self.args = args
        self.spicerack = spicerack

    @property
    def depool_services(self) -> list[str]:
        """Depool recdns or authdns-ns.* based on the name of the service."""
        # We depool all authdns-ns.* services.
        if self.args.service == 'haproxy':
            return ['authdns-ns.*']
        if self.args.service == 'pdns-recursor':
            return ['recdns']
        return ['recdns', 'authdns-ns.*']

    @property
    def allowed_aliases_query(self) -> str:
        """Optimize the query"""
        return 'A:dnsbox'

    @property
    def allowed_aliases(self) -> list:
        """Required by RebootRunnerBase"""
        return ['dnsbox', 'dns-rec', 'dns-auth']

    @property
    def restart_daemons(self) -> list:
        """List of daemons to restart"""
        if self.args.service == 'haproxy':
            return ['haproxy.service']
        if self.args.service == 'pdns-recursor':
            return ['pdns-recursor.service']
        return ['pdns-recursor.service', 'haproxy.service']
