"""Routing MediaWiki traffic between datacenters."""

import argparse
from typing import Optional

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack import Spicerack
from wmflib.constants import CORE_DATACENTERS
from wmflib.interactive import ask_confirmation
from cookbooks.sre.switchdc.mediawiki import MEDIAWIKI_RO_SERVICES


class RouteMediaWikiTraffic(CookbookBase):
    """Re-route read traffic to mediawiki either to active-active or to active-passive.

    Examples:
        Route read traffic to all core datacenters:
            cookbook sre.mediawiki.route-traffic all
        Route read traffic only to the primary MediaWiki datacenter:
            cookbook sre.mediawiki.route-traffic primary

    """

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "what",
            choices=["all", "primary"],
            help="Route traffic to all datacenters or just to the primary.",
        )
        return parser

    def get_runner(self, args) -> CookbookRunnerBase:
        """Get the runner."""
        return RouteMediaWikiTrafficRunner(args, self.spicerack)


class RouteMediaWikiTrafficRunner(CookbookRunnerBase):
    """Executor for the routing change."""

    def __init__(self, args: argparse.Namespace, spicerack: Spicerack):
        """Initialize the class with the current state."""
        self.all = args.what == "all"
        self.spicerack = spicerack
        self.primary_dc = spicerack.mediawiki().get_master_datacenter()
        self.discovery_records = spicerack.discovery(*MEDIAWIKI_RO_SERVICES)
        self.initial_state = self.discovery_records.active_datacenters

    @property
    def runtime_description(self) -> str:
        """What gets logged to SAL."""
        if self.all:
            return "routing MediaWiki read-only traffic to all datacenters"
        # I know. if / else would be more readable, but the stupid linter
        # thinks otherwise and adding comments to disable its kinks would make the code
        # equally unreadable. So I hope this comment provides enough padding
        # to provide reading ease.
        return f"routing MediaWiki read-only traffic to {self.primary_dc} only"

    def run(self) -> Optional[int]:
        """Route the traffic as requested."""
        # before proceeding, we need to ensure the records are pooled in the master DC
        # Please note: here we don't need to wait for this to propagate.
        # We're dealing with read-only traffic, it can move inconsistently over 5 minutes
        # and it's ok.
        self.discovery_records.pool(self.primary_dc)
        for dc in CORE_DATACENTERS:
            if dc == self.primary_dc:
                continue
            if self.all:
                self.discovery_records.pool(dc)
            else:
                self.discovery_records.depool(dc)
        return 0

    def rollback(self):
        """In case of error in the procedure, rollback to the previous state."""
        current_state = self.discovery_records.active_datacenters
        print(
            "There have been errors running the cookbook. This is the current situation:"
        )
        for svc in current_state:
            print(f"## {svc}.discovery.wmnet")
            for dc in CORE_DATACENTERS:
                if dc in self.initial_state[svc]:
                    old_st = "(was pooled)"
                else:
                    old_st = "(was depooled)"
                if dc in current_state[svc]:
                    print(f"\t{dc}: pooled {old_st}")
                else:
                    print(f"\t{dc}: depooled {old_st}")
            print("---")

        ask_confirmation(
            "Do you whish to rollback to the state before the cookbook ran?"
        )
        # first repool datacenters that have been depooled
        for svc, dcs in self.initial_state.items():
            disc = self.spicerack.discovery(svc)
            for dc in dcs:
                if dc not in current_state[svc]:
                    disc.pool(dc)
        # Now depool datacenters that have been pooled.
        for svc, dcs in current_state.items():
            disc = self.spicerack.discovery(svc)
            for dc in dcs:
                if dc not in self.initial_state[svc]:
                    disc.depool(dc)
