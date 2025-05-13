"""Cookbook for GeoDNS pool/depool of a site."""

from spicerack.cookbook import CookbookBase, CookbookInitSuccess, CookbookRunnerBase
from wmflib.constants import ALL_DATACENTERS, DATACENTER_NUMBERING_PREFIX, US_DATACENTERS
from wmflib.interactive import ask_confirmation

SERVICES = ("text-addrs", "text-next", "upload-addrs", "ncredir-addrs")

DEPOOL_THRESHOLD = .5


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

    argument_reason_required = False
    argument_task_required = False

    def argument_parser(self):
        """As specified by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("action", choices=("pool", "depool", "show"),
                            help="The kind of action to perform (pool, depool, or show).")
        parser.add_argument("site", choices=ALL_DATACENTERS, nargs="?",
                            help="The site/DC on which to perform the action on.")
        parser.add_argument("-s", "--service", choices=SERVICES, nargs="*",
                            help="The service in the site/DC on which the action should be performed.")
        parser.add_argument("-f", "--force", action="store_true",
                            help="If passed, do not prompt for any actions (default: prompt)")
        parser.add_argument("--emergency-depool-policy", action="store_true",
                            help="If passed, override the depool threshold and ignore all depool safety checks")
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

        # Before we proceed, print the current admin_state as seen by confctl.
        self._print_summary("CURRENT")

        if self.args.action == "show":
            raise CookbookInitSuccess("show action called; outputting current admin_state above. No changes were made.")

        # Safety checks before we actually call run() depool. These are skipped
        # further down if the override flag is passed.
        if self.args.action == "depool":
            self.safety_checks()

    def safety_checks(self):
        """Checks to perform before depooling a site/resources."""
        # Show the below without performing any checks because it was passed
        # after all and we need user confirmation, even if the changes won't
        # actually meet the thresholds.
        if self.args.emergency_depool_policy:
            print("WARNING: Emergency depool policy was set. Overriding any depool policy thresholds.\n"
                  "We will NOT stop executing the cookbook for any failing safety check. May the packets be with you.")
            ask_confirmation("I understand fully what I am doing and wish to continue.")

        # If --services was passed, use that otherwise use all SERVICES (equal to a full site).
        services_to_check = self.args.service if self.args.service is not None else SERVICES
        for service in services_to_check:
            # Simulate what happens when an actual action is performed.
            simulate_depool = [s.name for s in self.confctl.get(geodns=service) if s.pooled == "no"]
            # Maybe the given site is already depooled, in which case, ignore.
            if self.args.site not in simulate_depool:
                simulate_depool.append(self.args.site)

            depool_policy_msg = "\nYou can override this with --emergency-depool-policy BUT IT IS NOT RECOMMENDED."
            depool_continue_msg = "\nContinuing because --emergency-depool-policy was passed."

            # Check 1.
            # Do not allow depool beyond a certain threshold.
            if len(simulate_depool) / len(ALL_DATACENTERS) > DEPOOL_THRESHOLD:
                if not self.args.emergency_depool_policy:
                    raise RuntimeError(f"Cannot depool {service} for {self.args.site}.\n"
                                       f"Depool threshold exceeded as {service} "
                                       f"is already depooled for {len(simulate_depool)} sites."
                                       f"{depool_policy_msg}")
                print(f"Depool of {service} would have failed as depool threshold was exceeded."
                      f"{depool_continue_msg}")

            # Check 2.
            # Do not allow all US sites to be depooled.
            if set(US_DATACENTERS).issubset(simulate_depool):
                if not self.args.emergency_depool_policy:
                    raise RuntimeError(f"Cannot depool all US data centers {', '.join(US_DATACENTERS)} for {service}."
                                       f"{depool_policy_msg}")
                print("Depool would have failed as all US sites will be depooled."
                      f"{depool_continue_msg}")

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the DNS action."""
        return f"DNS admin: {self.action_string} [reason: {self.reason}, {self.task_id}]"

    def run(self):
        """Perform administrative DNS action on the given DC/service."""
        # We need a site for anything else other than "show" above.
        if self.args.site is None:
            raise RuntimeError(f"A site for {self.args.action} was not passed.")

        if not self.args.force:
            ask_confirmation(f"You are now about to: {self.action_string}")

        if self.service is not None:
            self.confctl.set_and_verify("pooled", self.pooled_state, geodns=self.service, name=self.args.site)
        else:
            self.confctl.set_and_verify("pooled", self.pooled_state, name=self.args.site)

        self._print_summary("APPLIED")

    def _print_summary(self, msg):
        print(f"==> {msg} STATE:")
        for service in SERVICES:
            service_site_status = [s.name for s in self.confctl.get(geodns=service) if s.pooled == "no"]
            depooled_sites = ", ".join(sorted(service_site_status, key=lambda num: DATACENTER_NUMBERING_PREFIX[num]))
            if not depooled_sites:
                print(f"{service}: pooled at all sites")
            else:
                print(f"{service}: depooled in {depooled_sites}")
        print("<==")
