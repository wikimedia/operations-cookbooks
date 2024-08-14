"""Cookbook for GeoDNS pool/depool of a site."""

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import ask_confirmation

SERVICES = ["text-addrs", "text-next", "upload-addrs", "ncredir-addrs"]


class DNSAdmin(CookbookBase):
    """Cookbook for GeoDNS pool/depool of a site.

    Pool or depool a site for GeoDNS. By default, it will act on a given site
    for all services (text-addrs, upload-addrs, etc.) unless a service is
    manually specified via --service.

    Usage examples:
        cookbook sre.dns.admin depool eqiad # [depools eqiad for everything]
        cookbook sre.dns.admin pool magru   # [pools magru for everything]
        cookbook sre.dns.admin --service upload-addrs -- depool codfw      # [depool codfw for upload-addrs]
        cookbook sre.dns.admin depool esams --service text-addrs text-next # [depool esams for text*]
    """

    def argument_parser(self):
        """As specified by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("action", choices=("pool", "depool", "show"),
                            help="The kind of action to perform (pool, depool, or show).")
        parser.add_argument("site", choices=ALL_DATACENTERS, nargs="?",
                            help="The site/DC on which to perform the action on.")
        parser.add_argument("-s", "--service", choices=SERVICES, nargs="*",
                            help="The service in the site/DC on which the action should be performed.")
        parser.add_argument("-r", "--reason",
                            help="An optional reason for the action.")
        parser.add_argument("-t", "--task-id",
                            help="An optional Phabricator task ID to log the action.")
        parser.add_argument("-f", "--force", action="store_true",
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
        self.service = "|".join(self.args.service) if self.args.service is not None else self.args.service

        self.action_string = (
            f"{self.args.action} site {self.args.site} for service: {self.service}"
            if self.service is not None
            else f"{self.args.action} site {self.args.site}"
        )

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the DNS action."""
        return f"DNS admin: {self.action_string} [reason: {self.reason}, {self.task_id}]"

    def run(self):
        """Perform administrative DNS action on the given DC/service."""
        # Before we proceed, print the current admin_state as seen by confctl.
        self._print_summary("=> CURRENT STATE:")

        if self.args.action == "show":
            return

        # We need a site for anything else other than "show" above.
        if self.args.site is None:
            raise RuntimeError(f"A site for {self.args.action} was not passed.")

        if not self.args.force:
            ask_confirmation(f"You are now about to: {self.action_string}")

        if self.service is not None:
            self.confctl.update({"pooled": self.pooled_state}, geodns=self.service, name=self.args.site)
        else:
            self.confctl.update({"pooled": self.pooled_state}, name=self.args.site)

        self._print_summary("=> APPLIED STATE:")

    def _print_summary(self, msg):
        print(msg)
        for service in SERVICES:
            service_site_status = [s.name for s in self.confctl.get(geodns=service) if s.pooled == "no"]
            depooled_sites = ", ".join(service_site_status)
            if not depooled_sites:
                print(f"{service}: pooled at all sites")
            else:
                print(f"{service}: depooled in {depooled_sites}")
