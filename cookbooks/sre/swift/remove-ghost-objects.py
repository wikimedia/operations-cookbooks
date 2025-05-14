"""Swift ghost object removal cookbook"""

import collections
import logging
import os
import os.path
import shlex
import socket
import sqlite3
import tempfile
import urllib.parse

import transferpy.transfer
from transferpy.Transferer import Transferer

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.constants import CORE_DATACENTERS
from wmflib.interactive import ask_input, ensure_shell_is_durable

from cookbooks.sre.swift import find_db_paths, lookup_be_host

logger = logging.getLogger(__name__)


class RemoveGhostObjects(CookbookBase):
    """Remove ghost objects from a swift container

    Ghost objects are objects that appear in a container listing
    but do not in fact exist. Typically in this case the container listing
    will differ each time in the number of ghost objects it lists.

    So instead we collect the three copies of the container database
    from the backend hosts on which they reside, and treat objects
    that are only in one copy of the database as potential ghosts.

    If we are working in the primary DC, then potential ghosts which
    are NOT present in the remote DC are considered for removal (we do
    not want a subsequent sync job to propogate the deletion of a
    ghost object and remove a real object from the remote DC).

    If we are working in the secondary DC, then all ghosts are removed
    (the next sync job will copy any corresponding real objects from
    remote to this DC).

    In either case, for each target ghost in turn, we do swift stat,
    and if that returns 404 we then immediately do swift delete and
    check that that returns 404. There is an unavoidable race here (an
    upload between the stat and delete calls), which we minimise by
    doing the stat-then-delete in a single shell pipeline. In the
    event of losing the race we stop immediately, which should allow
    for the uploaded object to be retrieved from the other DC.

    Example usage:
      cookbook sre.swift.remove-ghost-objects codfw wikipedia-commons-local-public.98

    See T327253 for more background. If in any doubt, please consult
    with the SRE Data Persistence team before using this cookbook.

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--eqiad-be-host',
                            help='fqdn of host in eqiad to query for container location')
        parser.add_argument('--codfw-be-host',
                            help='fqdn of host in codfw to query for container location')
        parser.add_argument('--fe-host',
                            help='fqdn of frontend host to run swift delete on')
        parser.add_argument('--workdir',
                            default=("~/.ghost_workdir"),
                            help='local directory to store dbs in ~ mapped to calling user')
        parser.add_argument('--no-clean-workdir',
                            action='store_true',
                            help='do not clean out workdir')
        parser.add_argument('--skip-db-fetch',
                            action='store_true',
                            help='assume container dbs already downloaded')
        parser.add_argument('--write-ghost-file',
                            action='store_true',
                            help='write a list of ghosts to a file in workdir before prompting to continue')
        parser.add_argument('working_dc',
                            choices=CORE_DATACENTERS,
                            help='remove ghosts in this dc')
        parser.add_argument('container',
                            help='name of container to operate on')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return RemoveGhostObjectsRunner(args, self.spicerack)


class RemoveGhostObjectsRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        # Make sure we are running in screen/tmux
        ensure_shell_is_durable()
        # best to extract bits of the spicerack.Spicerack instance that we need
        # rather than just self.spicerack = spicerack
        self.dns = spicerack.dns()
        self.remote = spicerack.remote()
        self.primary_dc = spicerack.mediawiki().get_master_datacenter()
        self.secondary_dc = list(set(CORE_DATACENTERS) - {self.primary_dc})[0]
        # args
        self.container = args.container
        self.no_clean_workdir = args.no_clean_workdir
        self.write_ghost_file = args.write_ghost_file
        self.ghost_file_path = None
        self.skip_db_fetch = args.skip_db_fetch
        self.working_dc = args.working_dc
        # working state, keyed on DC
        codfw_be_host = lookup_be_host(self.remote,
                                       "codfw", args.codfw_be_host)
        eqiad_be_host = lookup_be_host(self.remote,
                                       "eqiad", args.eqiad_be_host)
        self.state: dict = {"codfw": {"backend": codfw_be_host},
                            "eqiad": {"backend": eqiad_be_host}}
        if args.fe_host is not None:
            if self.working_dc not in args.fe_host:
                raise ValueError("Frontend host %s not in working DC %s" %
                                 (args.fe_host,
                                  self.working_dc))
            fe_query = args.fe_host
        else:
            fe_query = f"A:{self.working_dc} and P{{C:swift::stats_reporter%ensure = present}} and P{{O:swift::proxy}}"
        self.fe_host = self.remote.query(fe_query)
        if len(self.fe_host) > 1:
            raise ValueError("Should only specify 1 frontend host")
        # Other prep
        self.tp_options = dict(transferpy.transfer.parse_configurations(
            transferpy.transfer.CONFIG_FILE))
        # this also handles string->bool conversion where necessary
        self.tp_options = transferpy.transfer.assign_default_options(
            self.tp_options)
        # Some of our transfers are cross-DC, so enable encryption
        self.tp_options['encrypt'] = True
        # transferpy needs fqdn, not "localhost"
        self.localhost = socket.getfqdn()
        self.username = spicerack.username
        # If user specified ~/, make that ~ expand to their user home
        # rather than roots.
        self.workdir = os.path.expanduser(
            args.workdir.replace("~/", f"~{self.username}/"))

    @property
    def runtime_description(self):
        """Return a string presenting the cookbook action."""
        return f"from container {self.container} in {self.working_dc}"

    def run(self):
        """Run the cookbook."""
        if not os.path.isdir(self.workdir):
            os.mkdir(self.workdir)
        for dc in CORE_DATACENTERS:
            self._prep_state(dc)
        if self.state[self.working_dc]["consistent"] is True:
            logger.info("Container in working dc has no ghosts to remove")
            self._remove_dbs()
            return 0
        if self.working_dc == self.primary_dc:
            # Remove ghosts from 1ry DC that match absent (or ghosts) in 2ry DC
            # -> next rclone run will delete any corresponding ghost from 2ry
            logger.info("Working from primary DC.")
            logger.info("Will remove ghosts from %s that are not real objects in %s",
                        self.working_dc, self.secondary_dc)
            ghosts = self._generate_ghost_list(work_in_primary=True)
        else:
            # Remove all ghosts from 2ry DC
            # -> next rclone run will copy the real object (if present) from 1ry
            logger.info("Working from secondary DC.")
            logger.info("Will remove ALL ghosts from %s",
                        self.working_dc)
            ghosts = self._generate_ghost_list(work_in_primary=False)
        if len(ghosts) > 0:
            if self.write_ghost_file:
                with tempfile.NamedTemporaryFile(delete=False,
                                                 dir=self.workdir,
                                                 mode="w") as fp:
                    self.ghost_file_path = fp.name
                    for ghost in ghosts:
                        print(ghost, file=fp)
                logger.info("Ghost names written to %s", self.ghost_file_path)
            ans = ask_input(f"OK to DELETE {len(ghosts)} ghosts from {self.container}?",
                            ("yes", "no"))
            if ans == "yes":
                for ghost in ghosts:
                    self._delete_ghost(self.fe_host, self.container, ghost)
        else:
            logger.info("No ghosts found to delete")
        self._remove_dbs()
        return 0

    def _remove_dbs(self):
        """Unless self.no_clean_workdir set, remove databases from workdir"""
        if self.no_clean_workdir:
            return
        for dc in self.state.values():
            for fqdn in dc["nodes"]:
                sn = fqdn.split('.')[0]
                os.remove(f"{self.workdir}/{sn}.db")
        if self.ghost_file_path is not None:
            os.remove(self.ghost_file_path)

    def _get_both_dbs(self, dc):  # pylint: disable=invalid-name
        """get_both_dbs - return (consensus, divergent) db hosts for dc

        If all 3 DBs agree, return (consensus, None). We assume that the
        divergent database has more entries (i.e. ghosts) in.
        Raise a ValueError if that isn't the case.
        """
        if self.state[dc]["consistent"] is True:
            return (self.state[dc]["nodes"][0], None)
        counts = collections.defaultdict(list)
        count_by_node = {}
        for node in self.state[dc]["nodes"]:
            count = self._get_single_value(node,
                                           "SELECT COUNT(*) FROM object WHERE deleted == 0")
            counts[count].append(node)
            count_by_node[node] = count
        if len(counts) == 1:  # pylint: disable=no-else-raise
            raise ValueError(f"{dc} state non-consistent but all 3 dbs agree")
        elif len(counts) == 2:
            hosts_by_count = {len(v): k for k, v in counts.items()}
            divergent_host = counts[hosts_by_count[1]][0]
            consensus_host = counts[hosts_by_count[2]][0]
            if count_by_node[divergent_host] <= count_by_node[consensus_host]:
                raise ValueError(f"Divergent node {divergent_host} less objects than consensus node {consensus_host}")
        elif len(counts) == 3:
            logger.warning("All three nodes have different counts: %s", counts)
            divergent_host = counts[max(counts)][0]
            consensus_host = counts[min(counts)][0]
            logger.warning("Will treat objects in %s and not %s as possible ghosts",
                           divergent_host, consensus_host)
        else:
            raise ValueError(f"Impossible counts value: {counts}")
        return (consensus_host, divergent_host)

    def _get_consensus_db(self, dc):  # pylint: disable=invalid-name
        """Return consensus db host for dc"""
        return self._get_both_dbs(dc)[0]

    def _generate_ghost_list(self, *, work_in_primary):
        """Generate a list of ghosts to remove

        we're removing objects from NEAR depending on their state in FAR

        nl is the larger NEAR database, nc the smaller

        We do a LEFT JOIN on undeleted objects to find those in nl but
        not in nc, these are the ghosts.

        If working in the primary DC, we return ghosts with no
        (undeleted) entry in remote - we must NOT remove ghosts that
        do correspond with an entry in remote, as otherwise the next
        sync job will propagate that deletion and remove the real
        entry in remote.

        If working in the secondary DC, we return all the ghosts - any
        that are already absent in primary are already gone, any that
        are still in primary will be copied over by the next sync job.

        Returns a list of (unquoted) ghosts for deletion

        """
        db = sqlite3.connect(":memory:")
        cursor = db.cursor()
        if work_in_primary:
            fdb = self._get_consensus_db(self.secondary_dc)
            near_consensus, near_large = self._get_both_dbs(self.primary_dc)
        else:
            fdb = self._get_consensus_db(self.primary_dc)
            near_consensus, near_large = self._get_both_dbs(self.secondary_dc)
        # Attach our databases (read-only)
        self._attach_ro_db(cursor, near_consensus, "nc")
        self._attach_ro_db(cursor, near_large, "nl")
        self._attach_ro_db(cursor, fdb, "fdb")
        query = """
        WITH ghosts AS (
          WITH small AS (SELECT * FROM nc.object WHERE deleted == 0),
               large AS (SELECT * FROM nl.object WHERE deleted == 0)
            SELECT * FROM large LEFT JOIN small USING(name)
            WHERE small.name IS NULL
        ),
        remote AS (SELECT * FROM fdb.object WHERE deleted == 0)
        """
        # If working from primary DC, delete ghosts with no entry in remote
        if work_in_primary:
            query += """SELECT name FROM ghosts LEFT JOIN remote USING(name)
            WHERE remote.name IS NULL"""
        # Otherwise, delete all ghosts in container
        else:
            query += """SELECT name FROM ghosts"""
        cursor.execute(query)
        # each result is returned from fetchall() as a 1-member tuple
        res = [row[0] for row in cursor.fetchall()]
        db.close()
        return res

    def _delete_ghost(self, host, container, ghost):
        """Use swift CLI on host to remove ghost from container

        ghost should not be url-encoded nor shell-quoted
        Check swift stat returns 404 and then that swift delete also does so.
        """
        if len(ghost) == 0:
            raise ValueError("Ghost must be a non-empty string")
        logger.debug("Deleting %s", ghost)
        gq = shlex.quote(ghost)
        cmd = ". /etc/swift/account_AUTH_mw.env ; "
        cmd += f"if swift stat {container} {gq} 2>&1 | grep -q '404 Not Found' ; "
        cmd += f"then swift delete {container} {gq} 2>&1 | grep -q '404 Not Found' ; "
        # Make sure we return non-zero if the first grep doesn't match
        cmd += "else echo 'swift stat did not return 404'; false ; fi"
        try:
            host.run_sync(cmd,
                          is_safe=False,
                          print_output=False,
                          print_progress_bars=False)
        except Exception:
            logger.error("Removal of %s failed", ghost)
            ug = urllib.parse.quote(ghost)
            logger.error("Suggest grepping swift logs for %s", ug)
            raise

    def _prep_state(self, dc):  # pylint: disable=invalid-name
        """Locate container DBs, fetch, check consistency"""
        dbs = find_db_paths(self.dns,
                            self.state[dc]["backend"], self.container)
        if not self.skip_db_fetch:
            logger.info("Fetching container dbs from %s nodes", dc)
            for fqdn, path in dbs:
                self._fetch_db(self.workdir, fqdn, path)
        self._check_timestamps_equal(dbs)
        self.state[dc]["nodes"] = [x[0] for x in dbs]
        self.state[dc]["consistent"] = self._test_consistent(dbs)

    def _get_single_value(self, fqdn, query):
        """Run query on db from fqdn, return single value

        assumes presence of shorthostname.db in self.workdir
        raises ValueError if >1 value returned
        """
        sn = fqdn.split('.')[0]
        db = sqlite3.connect(f"file:{self.workdir}/{sn}.db?mode=ro",
                             uri=True)
        ans = db.execute(query).fetchall()
        db.close()
        if len(ans) != 1 or len(ans[0]) != 1:
            raise ValueError(f"expected single return from {query}, got {ans}")
        return ans[0][0]

    def _attach_ro_db(self, cursor, fqdn, name):
        """Attach fqdn's db ro as name in cursor"""
        sn = fqdn.split('.')[0]
        query = f"ATTACH DATABASE 'file:{self.workdir}/{sn}.db?mode=ro' as {name}"
        cursor.execute(query)

    def _check_single_value_consistent(self, dbs, query):
        """Run query against each db, return True if all the same"""
        # Create a set object with the value from each query
        # its length is the number of distinct values returned
        vals = {self._get_single_value(d[0], query) for d in dbs}
        return len(vals) == 1

    def _test_consistent(self, dbs):
        """Check each database is consistent in terms of object_count"""
        query = "SELECT object_count FROM container_stat"
        return self._check_single_value_consistent(dbs, query)

    def _check_timestamps_equal(self, dbs):
        """Check the put_timestamp for each database is the same"""
        query = "SELECT put_timestamp FROM container_info"
        if self._check_single_value_consistent(dbs, query) is False:
            raise RuntimeError("Timestamps on container DBs do not match")

    def _fetch_db(self, workdir, fqdn, path):
        """Copy path from fqdn to workdir, rename to shorthostname.db"""
        t = Transferer(fqdn, path, [self.localhost], [workdir], self.tp_options)
        # transfer.py produces a lot of log chatter, cf T330882
        logger.debug("Starting transferpy, expect cumin errors")
        r = t.run()
        logger.debug("Transferpy complete")
        if r[0] != 0:
            raise RuntimeError(f"Transfer of {path} from {fqdn} failed")
        sn = fqdn.split('.')[0]
        bp = os.path.basename(path)
        os.rename(f"{workdir}/{bp}", f"{workdir}/{sn}.db")
