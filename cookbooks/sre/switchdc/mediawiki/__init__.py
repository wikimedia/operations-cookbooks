"""Switch Datacenter for MediaWiki

In order to keep this cookbook up to date, please take a look at service::catalog in Puppet, to
ensure that the list of services is accurate and up to date.
"""

import argparse

from abc import ABCMeta

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.constants import CORE_DATACENTERS


__title__ = __doc__
# TODO: Consider replacing PUPPET_REASON the administrative reason.
PUPPET_REASON = __name__
DNS_SHORT_TTL = 10  # DNS short TTL in seconds to use during the switchdc
DEFAULT_READ_ONLY_REASON = (
    "You can't edit now. This is because of maintenance. Copy and save your text and try again "
    "in a few minutes."
)
MEDIAWIKI_SERVICES = (
    "jobrunner",
    "mwdebug",
    "mw-web",
    "mw-api-ext",
    "mw-api-int",
    "mw-jobrunner",
    "mw-parsoid",
    "mw-wikifunctions",
    "videoscaler",
)
# Read-only mediawiki services that are active-active by default and won't be touched by this switchover.
MEDIAWIKI_RO_SERVICES = (
    "mw-web-ro",
    "mw-api-ext-ro",
    "mw-api-int-ro",
    "mw-wikifunctions-ro",
    "mw-misc",
)
# Regex matching services to downtime, when disabling read-only checks on the MariaDB primaries. The blank is for the
# section name, e.g. "MariaDB read only s1 #page".
READ_ONLY_SERVICE_RE = r"MariaDB read only \S+ #page"


class MediaWikiSwitchDCBase(CookbookBase, metaclass=ABCMeta):
    """A common CookbookBase class for MediaWiki switchover cookbooks.

    Subclasses must define the runner_class class attribute.
    """

    runner_class: type["MediaWikiSwitchDCRunnerBase"]
    """The MediaWikiSwitchDCRunnerBase subclass type to construct in get_runner."""

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--ro-reason",
            default=DEFAULT_READ_ONLY_REASON,
            help="The read-only reason message to set in Conftool.",
        )
        parser.add_argument(
            "--live-test",
            action="store_true",
            help=(
                "Perform a live test assuming that DC_TO is already the active datacenter and DC_FROM is "
                "already the passive datacenter. Automatically skip or invert, when feasible, the steps "
                "that will disrupt DC_TO if they were run."
            ),
        )
        # TODO: Add a task ID.
        parser.add_argument(
            "dc_from",
            metavar="DC_FROM",
            choices=CORE_DATACENTERS,
            help="Name of the datacenter to switch away from. One of: %(choices)s.",
        )
        parser.add_argument(
            "dc_to",
            metavar="DC_TO",
            choices=CORE_DATACENTERS,
            help="Name of the datacenter to switch to. One of: %(choices)s.",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> "MediaWikiSwitchDCRunnerBase":
        """Returns a runner instance."""
        return self.runner_class(args, self.spicerack)


class MediaWikiSwitchDCRunnerBase(CookbookRunnerBase, metaclass=ABCMeta):
    """A common CookbookRunnerBase class for MediaWiki switchover cookbooks.

    As per the Spicerack CookbookRunnerBase API, subclasses must implement at least the run method.
    """

    # Switchover cookbooks should run exclusively and the longest-running cookbook should complete in ~ 5m.
    max_concurrency = 1
    lock_ttl = 600  # Set a backstop lock expiration of 2x the longest-running cookbook.

    def __init__(self, args: argparse.Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        if args.dc_from == args.dc_to:
            raise ValueError("DC_FROM and DC_TO must differ")

        self.ro_reason = args.ro_reason
        self.live_test = args.live_test
        self.dc_to = args.dc_to
        self.dc_from = args.dc_from

        self.spicerack = spicerack

    @property
    def runtime_description(self) -> str:
        """Runtime description for logging purposes (e.g., SAL)."""
        return f"for datacenter switchover from {self.dc_from} to {self.dc_to}"
