"""Cookbook for GeoDNS pool/depool of a site."""

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import ask_confirmation


class DNSAdmin(CookbookBase):
    """Cookbook for GeoDNS pool/depool of a site.

    Pool or depool a site for GeoDNS. By default, it will act on a given site
    for all services (text-addrs, upload-addrs, etc.) unless a service is
    manually specified via --service.

    Usage examples:
        cookbook sre.dns.admin pool eqiad # [depools eqiad for everything]
        cookbook sre.dns.admin depool magru
        cookbook sre.dns.admin --service upload-addrs depool codfw # [depool codfw for upload-addrs]
    """

    def argument_parser(self):
        """As specified by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("action", choices=("pool", "depool"),
                            help="The kind of action to perform (pool or depool).")
        parser.add_argument("site", choices=ALL_DATACENTERS,
                            help="The site/DC on which to perform the action on.")
        parser.add_argument("-s", "--service", choices=("text-addrs", "text-next", "upload-addrs", "ncredir-addrs"),
                            help="The service in the site/DC on which the action should be performed.")
        parser.add_argument("-r", "--reason",
                            help="An optional reason for the action.")
        parser.add_argument("-t", "--task-id",
                            help="An optional Phabricator task ID to log the action.")
        parser.add_argument("-f", "--force", action='store_true',
                            help="If passed, do not prompt for any actions (default: prompt)")
        return parser

    def get_runner(self, args):
        """Required by Spicerack API."""
        return DNSAdminRunner(args, self.spicerack)


class DNSAdminRunner(CookbookRunnerBase):
    """Pool or a depool a site for GeoDNS."""

    # We only one want one concurrent run for the cookbook with a short TTL.
    max_concurrency = 1
    lock_ttl = 60

    def __init__(self, args, spicerack):
        """Initialize DNSAdminRunner."""
        self.args = args
        self.spicerack = spicerack
        self.confctl = spicerack.confctl("geodns")

        self.pooled_state = "yes" if self.args.action == "pool" else "no"
        self.reason = self.args.reason if self.args.reason is not None else "no reason specified"
        self.task_id = self.args.task_id if self.args.task_id is not None else "no task ID specified"

        self.action_string = (
            f"{self.args.action} site {self.args.site} for service: {self.args.service}"
            if self.args.service is not None
            else f"{self.args.action} site {self.args.site}"
        )

        if not self.args.force:
            ask_confirmation(self.action_string)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the DNS action."""
        return f"DNS admin: {self.action_string} [reason: {self.reason}, {self.task_id}]"

    def run(self):
        """Perform administrative DNS action on the given DC/service."""
        if self.args.service is not None:
            self.confctl.update({"pooled": self.pooled_state}, geodns=self.args.service, name=self.args.site)
        else:
            self.confctl.update({"pooled": self.pooled_state}, name=self.args.site)
