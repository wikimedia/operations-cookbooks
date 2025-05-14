"""Swift cookbook to perform integrity check on container DBs"""

import logging
import shlex
from typing import cast

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteHosts, RemoteExecutionError
from wmflib.constants import CORE_DATACENTERS

from cookbooks.sre.swift import find_db_paths, lookup_be_host, lookup_fe_host

logger = logging.getLogger(__name__)


class CheckContainerDBs(CookbookBase):
    """Check the database files of a swift container

    Each swift container has 3 sqlite3 database files on-disk. This
    cookbook uses the integrity_check PRAGMA to check that those files
    are in a coherent state. It only performs read-only operations.

    Example usage:
      cookbook sre.swift.check-dbs wikipedia-commons-local-public.98

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--eqiad-be-host',
                            help='host name in eqiad to query for container location')
        parser.add_argument('--codfw-be-host',
                            help='host name in codfw to query for container location')
        parser.add_argument('--fe-host',
                            help='host name of frontend host to use to confirm container existence')
        parser.add_argument('--assume-container-exists',
                            action='store_true',
                            help='omit check that supplied container name exists')
        parser.add_argument('--dc',
                            choices=CORE_DATACENTERS,
                            help='confine check to one DC')
        parser.add_argument('container',
                            help='name of container to operate on')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return CheckContainerDBsRunner(args, self.spicerack)


class CheckContainerDBsRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        # best to extract bits of the spicerack.Spicerack instance that we need
        # rather than just self.spicerack = spicerack
        self.dns = spicerack.dns()
        self.remote = spicerack.remote()
        # args
        self.container = shlex.quote(args.container)
        self.assume_container_exists = args.assume_container_exists
        if args.dc:
            self.working_dcs: tuple[str, ...] = (cast(str, args.dc),)
        else:
            self.working_dcs = CORE_DATACENTERS
        self.backends = {}
        if "codfw" in self.working_dcs:
            self.backends["codfw"] = lookup_be_host(self.remote,
                                                    "codfw", args.codfw_be_host)
        if "eqiad" in self.working_dcs:
            self.backends["eqiad"] = lookup_be_host(self.remote,
                                                    "eqiad", args.eqiad_be_host)
        if not self.assume_container_exists:
            self.frontend = lookup_fe_host(self.remote, args.dc, args.fe_host)

    @property
    def runtime_description(self):
        """Runtime description for IRC/SAL logging"""
        return f"Checking container DBs of {self.container}"

    def run(self):
        """Run the cookbook."""
        self._check_container_exists()
        checked = 0
        errors = 0
        # Check each DB, one at a time
        for dc in self.working_dcs:
            dbs = find_db_paths(self.dns, self.backends[dc],
                                self.container)
            for fqdn, path in dbs:
                checked += 1
                logger.debug("Checking %s on %s", path, fqdn)
                results = self.remote.query(fqdn).run_sync(
                    f"/usr/bin/sqlite3 --readonly {path} 'PRAGMA integrity_check'",
                    is_safe=True,
                    print_output=False,
                    print_progress_bars=False)
                ans = RemoteHosts.results_to_list(results)[0][1]
                if ans != "ok":
                    logger.error("%s on %s has errors:", path, fqdn)
                    logger.error(ans)
                    errors += 1
        logger.info("Checked %d dbs for container %s.", checked,
                    self.container)
        if errors > 0:
            logger.info("%d errors found, check cookbook log for details", errors)
            return 1
        logger.info("all containers checked OK.")
        return None

    def _check_container_exists(self):
        if self.assume_container_exists:
            logger.debug("Skipping container existence test as instructed")
            return
        logger.debug("Checking (on %s) that container %s exists",
                     self.frontend, self.container)
        cmd = ". /etc/swift/account_AUTH_mw.env ; "
        cmd += f"swift stat {self.container} >/dev/null"
        rh = self.remote.query(f"D{{{self.frontend}}}")
        try:
            rh.run_sync(cmd,
                        is_safe=True,
                        print_output=False,
                        print_progress_bars=False)
        # Give the user a hint :)
        except RemoteExecutionError:
            logger.error("Check for existence of container %s failed", self.container)
            raise
        # If that returned 0, the container exists
