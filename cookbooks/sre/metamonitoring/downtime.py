"""Downtime metamonitoring public-endpoint checks.

We need to downtime the service that does the metamonitoring
healthchecks directly as the result of the checks comes from an
external service. We do this by dropping a file, which the
metamonitoring service checks for and uses the timestamp to determine
whether it's in a downtime.

A downtime is a JSON file under `STATUS_DIR/downtimes/`` named
``<service>:<module>.json`` with the structure understood by the endpoint's
``active_downtime()``::

    {
        "expiry": <unix timestamp>,
        "reason": "a good reason",
        "author": "a good operator",
        "created": <unix timestamp>
    }

The endpoint is served from every alerting host (role::alerting_host), and a
downtime file is local to the host it is written on, so by default this cookbook
writes the downtime to all of them.
"""
import json
import logging
import shlex
import time

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation


logger = logging.getLogger(__name__)

# The hosts serving the public endpoint (role::alerting_host).
DEFAULT_QUERY = "O:alerting_host"
# default metamonitoring user
ENDPOINT_USER = "prometamon"
# profile::metamonitoring::status_dir
DOWNTIMES_DIR = "/var/lib/o11y-metamonitoring/downtimes"

# from SUPPORTED_SERVICES / KNOWN_MODULES in
# metamonitoring_public_endpoint.py
KNOWN_SERVICES = ["prometheus", "thanos", "icinga"]
KNOWN_MODULES = ["deadmanswitchnotified", "deadmanswitchonamdb", "extmon"]

DEFAULT_DOWNTIME_HOURS = 2


class Downtime(CookbookBase):
    """Downtime (or remove a downtime for) a metamonitoring public-endpoint check.

    Usage example:
      cookbook sre.metamonitoring.downtime -r 'thanos maintenance' thanos deadmanswitchonamdb
      cookbook sre.metamonitoring.downtime -r 'icinga upgrade' -H 4 icinga all
      cookbook sre.metamonitoring.downtime --remove thanos deadmanswitchonamdb
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "service",
            choices=KNOWN_SERVICES,
            help="The metamonitoring service to downtime.",
        )
        parser.add_argument(
            "module",
            choices=KNOWN_MODULES + ["all"],
            help="The check module to downtime.",
        )
        parser.add_argument(
            "-M",
            "--minutes",
            type=int,
            default=0,
            help="For how many minutes the downtime should last.",
        )
        parser.add_argument(
            "-H",
            "--hours",
            type=int,
            default=0,
            help="For how many hours the downtime should last.",
        )
        parser.add_argument(
            "-D",
            "--days",
            type=int,
            default=0,
            help="For how many days the downtime should last.",
        )
        parser.add_argument(
            "--remove",
            action="store_true",
            help="Remove the downtime instead of creating it (ignores duration/reason length).",
        )
        parser.add_argument(
            "--query",
            default=DEFAULT_QUERY,
            help=f"Cumin query selecting the hosts to act upon. (default={DEFAULT_QUERY})",
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        if not args.remove and not any((args.minutes, args.hours, args.days)):
            logger.info(
                "No downtime length option specified, using default value of %d hours",
                DEFAULT_DOWNTIME_HOURS,
            )
            args.hours = DEFAULT_DOWNTIME_HOURS

        return DowntimeRunner(args, self.spicerack)


class DowntimeRunner(CookbookRunnerBase):
    """Metamonitoring downtime cookbook runner class."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.remove = args.remove
        self.service = args.service
        self.modules = KNOWN_MODULES if args.module == "all" else [args.module]
        self.duration = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)

        self.remote_hosts = spicerack.remote().query(args.query)

        routes = ", ".join(f"{self.service}/{module}" for module in self.modules)
        action = "Remove downtime" if self.remove else f"Downtime for {self.duration}"
        self.short_message = (
            f"{action} of {routes} on {len(self.remote_hosts)} host(s) with reason: {self.reason.reason}"
        )

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return self.short_message

    def run(self):
        """Required by Spicerack API."""
        if self.remove:
            self._remove()
        else:
            self._add()

    def _add(self):
        """Write the downtime file for each module on every target host."""
        now = int(time.time())
        expiry = now + int(self.duration.total_seconds())
        until = datetime.now(timezone.utc) + self.duration
        payload = {
            "expiry": expiry,
            "reason": self.reason.reason,
            "author": self.reason.owner,
            "created": now,
        }
        # quote the contents of the json blob just in case there are weird chars
        content = shlex.quote(json.dumps(payload, indent=2) + "\n")

        ask_confirmation(f"Downtime {self.service}/{', '.join(self.modules)} until {until} " f"on {self.remote_hosts}?")

        for module in self.modules:
            path = f"{DOWNTIMES_DIR}/{self.service}:{module}.json"
            # The downtimes dir is owned by the endpoint user; write the file as
            # that user so the endpoint can always read it back.
            cmd = (
                f"printf '%s' {content} | "
                f"install -o {ENDPOINT_USER} -g {ENDPOINT_USER} -m 0644 "
                f"/dev/stdin {path}"
            )
            self.remote_hosts.run_sync(cmd)
            logger.info("Downtimed %s/%s until %s", self.service, module, until)

    def _remove(self):
        """Remove the downtime file for each module on every target host."""
        for module in self.modules:
            path = f"{DOWNTIMES_DIR}/{self.service}:{module}.json"
            self.remote_hosts.run_sync(f"/usr/bin/rm -v {path}")
            logger.info("Removed downtime for %s/%s", self.service, module)
