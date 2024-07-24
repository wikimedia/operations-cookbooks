"""Switch Datacenter for MediaWiki

In order to keep this cookbook up to date, please take a look at service::catalog in Puppet, to
ensure that the list of services is accurate and up to date.
"""

import argparse

from wmflib.constants import CORE_DATACENTERS


__title__ = __doc__
PUPPET_REASON = __name__
DNS_SHORT_TTL = 10  # DNS short TTL in seconds to use during the switchdc
DEFAULT_READ_ONLY_REASON = (
    "You can't edit now. This is because of maintenance. Copy and save your text and try again "
    "in a few minutes."
)
MEDIAWIKI_SERVICES = (
    "api-rw",
    "appservers-rw",
    "jobrunner",
    "mwdebug",
    "mw-web",
    "mw-api-ext",
    "mw-api-int",
    "mw-jobrunner",
    "mw-parsoid",
    "mw-wikifunctions",
    "parsoid-php",
    "videoscaler",
)
# Read-only mediawiki services that are active-active by default and won't be touched by this switchover.
MEDIAWIKI_RO_SERVICES = (
    "api-ro",
    "appservers-ro",
    "mw-web-ro",
    "mw-api-ext-ro",
    "mw-api-int-ro",
    "mw-wikifunctions-ro",
    "mw-misc",
)
# Regex matching services to downtime, when disabling read-only checks on the MariaDB primaries. The blank is for the
# section name, e.g. "MariaDB read only s1 #page".
READ_ONLY_SERVICE_RE = r"MariaDB read only \S+ #page"


def argument_parser_base(name, title):
    """Parse the command line arguments for all the sre.switchdc.mediawiki cookbooks."""
    parser = argparse.ArgumentParser(
        prog=name,
        description=title,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
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


def post_process_args(args):
    """Do any post-processing of the parsed arguments."""
    if args.dc_from == args.dc_to:
        raise ValueError("DC_FROM and DC_TO must differ")
