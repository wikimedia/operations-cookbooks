"""Check stat boxes, deployment servers, HDFS and Hive for data a departing user may have left behind."""
import json
import re
from argparse import ArgumentTypeError
from datetime import datetime, timezone
from logging import getLogger
from typing import Optional

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteError, RemoteExecutionError, RemoteHosts


logger = getLogger(__name__)

USERNAME_RE = re.compile(r'^[A-Za-z0-9_.-]+$')

# Above this many entries found on a single host, collapse the listing into a per-subdirectory
# summary instead of naming every single file, to keep the report readable.
DEFAULT_MAX_ENTRIES = 200

STAT_HOSTS_QUERY = 'stat1*'
DEPLOY_HOSTS_QUERY = 'deploy*'
HDFS_HOSTS_QUERY = 'A:an-launcher'

# type|size|mtime|ctime|path, matching the fields consumed by parse_find_output().
FIND_PRINTF = r'%y|%s|%T@|%C@|%p\n'

# Appended unconditionally to the end of every remote command (see run()) purely so that there is
# always at least one byte of output. It says nothing about whether anything was found: a command
# that finds nothing exits 0 with zero bytes of output, and Cumin's ClusterShell backend drops
# hosts with zero-byte output from the results entirely instead of reporting them as empty.
EMPTY_OUTPUT_GUARD = 'CUMIN-EMPTY-OUTPUT-GUARD'

HDFS_LS_RE = re.compile(
    r'^(?P<perms>[dl-][rwx-]{9})\s+\S+\s+(?P<owner>\S+)\s+(?P<group>\S+)\s+'
    r'(?P<size>\d+)\s+(?P<date>\S+)\s+(?P<time>\S+)\s+(?P<path>.+)$'
)


def validate_username(value: str) -> str:
    """Validate that a username is safe to interpolate into a remote shell command."""
    if not USERNAME_RE.match(value):
        raise ArgumentTypeError(f'invalid username {value!r}, must match {USERNAME_RE.pattern!r}')
    return value


def _epoch_to_iso(value: str) -> str:
    """Convert a find(1) %T@/%C@ epoch (with fractional seconds) to an ISO-8601 UTC timestamp."""
    return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()


def parse_find_output(raw: str) -> list[dict]:
    """Parse '<type>|<size>|<mtime>|<ctime>|<path>' lines produced by find(1) -printf."""
    entries = []
    for line in raw.splitlines():
        if not line or line == EMPTY_OUTPUT_GUARD:
            continue
        try:
            ftype, size, mtime, ctime, path = line.split('|', 4)
        except ValueError:
            logger.warning('Unparseable find output line: %s', line)
            continue
        entries.append({
            'path': path,
            'type': ftype,
            'size': int(size),
            'mtime': _epoch_to_iso(mtime),
            'ctime': _epoch_to_iso(ctime),
        })
    return entries


def parse_hdfs_ls_output(raw: str) -> list[dict]:
    """Parse the output of `hdfs dfs -ls`/`-ls -R` into structured entries."""
    entries = []
    for line in raw.splitlines():
        match = HDFS_LS_RE.match(line)
        if not match:
            continue
        entries.append({
            'path': match['path'],
            # TODO: maybe we could also distinguish non-dir-non-file nodes
            # (e.g. sockets) here, but I am not sure it would be useful enough
            # to warrant the somewhat complex code (just matching "s" won't work).
            'type': 'd' if match['perms'][0] == 'd' else 'f',
            'size': int(match['size']),
            'owner': match['owner'],
            'group': match['group'],
            'mtime': f"{match['date']}T{match['time']}",
        })
    return entries


def summarize_entries(entries: list[dict], root: str, max_entries: int) -> dict:
    """Collapse a large entry list into per-top-level-subdirectory counts and sizes."""
    if len(entries) <= max_entries:
        return {'summarized': False, 'entries': entries}

    by_subdir: dict[str, dict] = {}
    for entry in entries:
        rel = entry['path'][len(root):].lstrip('/')
        subdir = rel.split('/', 1)[0] if rel else '.'
        bucket = by_subdir.setdefault(subdir, {'directory': subdir, 'count': 0, 'total_size': 0})
        bucket['count'] += 1
        bucket['total_size'] += entry['size']

    return {
        'summarized': True,
        'total_entry_count': len(entries),
        'entries': sorted(by_subdir.values(), key=lambda item: item['directory']),
    }


class CheckUserLeftovers(CookbookBase):
    """Check for data a former staff member or volunteer may have left behind on WMF infrastructure.

    Looks for home directories and archived home directories on the stat boxes, home directories on
    the deployment servers, and personal directories/tables on HDFS and in Hive, and
    reports a per-host, per-check listing of what was found (paths, sizes, ctimes/mtimes).

    The exit code only reflects whether the checks themselves could be run (e.g. a hosts query
    failed to resolve); it does not signal whether any leftover data was found. Check the JSON
    report's "found"/"unreachable_hosts" fields for that.

    Usage example:
        cookbook sre.hadoop.check-user-leftovers jdoe
        cookbook sre.hadoop.check-user-leftovers --output /tmp/jdoe-leftovers.json jdoe
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('username', type=validate_username, help='the username to check for')
        parser.add_argument(
            '-o', '--output', help='path to write the JSON report to, in addition to printing a summary to the log'
        )
        parser.add_argument(
            '--max-entries', type=int, default=DEFAULT_MAX_ENTRIES,
            help=("collapse a host's listing into a per-subdirectory summary once it has more than this many "
                  "entries (default: %(default)s)"),
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return CheckUserLeftoversRunner(args, self.spicerack)


class CheckUserLeftoversRunner(CookbookRunnerBase):
    """Runner to check for leftover user data across stat boxes, deploy servers, HDFS and Hive."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.username = args.username
        self.output = args.output
        self.max_entries = args.max_entries
        self.remote = spicerack.remote()
        self.checks: list[dict] = []

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the check being performed."""
        return f'Checking for leftover data for user {self.username}'

    def _run_check(self, name: str, hosts_query: str, command: str, parser, root: Optional[str] = None) -> dict:
        """Run a single leftover check across the hosts matching hosts_query and return its report entry."""
        try:
            hosts: RemoteHosts = self.remote.query(hosts_query)
        except RemoteError as e:
            logger.warning('%s: unable to resolve hosts query %r: %s', name, hosts_query, e)
            return {'name': name, 'hosts_query': hosts_query, 'error': str(e)}

        per_host: dict[str, dict] = {}
        targeted = hosts.hosts
        try:
            results = hosts.run_sync(command, is_safe=True, print_output=False, print_progress_bars=False)
        except RemoteExecutionError as e:
            # Some hosts may still have produced output even though the overall run is reported as
            # failed (e.g. because a host without the checked-for path is genuinely unreachable);
            # recover whatever we can instead of discarding every host's result.
            logger.warning('%s: cumin execution error, using partial results: %s', name, e)
            results = e.results

        for nodeset, output in results:
            raw = output.message().decode(errors='replace')
            entries = parser(raw)
            for hostname in nodeset:
                if entries:
                    host_report = {'found': True, **summarize_entries(entries, root or '', self.max_entries)}
                else:
                    host_report = {'found': False}
                per_host[hostname] = host_report

        unreachable = sorted(str(h) for h in targeted if str(h) not in per_host)

        return {
            'name': name,
            'hosts_query': hosts_query,
            'hosts_checked': len(targeted),
            'unreachable_hosts': unreachable,
            'results': per_host,
        }

    def run(self):
        """Required by Spicerack API."""
        username = self.username

        # Every command below unconditionally (via `;`, not `&&`/`||`) echoes EMPTY_OUTPUT_GUARD at
        # the end. See its definition for why: without it, hosts where nothing is found vanish from
        # the results instead of being reported as empty.
        self.checks = [
            self._run_check(
                'Statbox homedir', STAT_HOSTS_QUERY,
                f'find "/home/{username}" -printf \'{FIND_PRINTF}\' 2>/dev/null; echo {EMPTY_OUTPUT_GUARD}',
                parse_find_output, root=f'/home/{username}',
            ),
            self._run_check(
                'Statbox userarchive', STAT_HOSTS_QUERY,
                f'find /var/userarchive -maxdepth 1 -iname "*{username}*" -printf \'{FIND_PRINTF}\' '
                f'2>/dev/null; echo {EMPTY_OUTPUT_GUARD}',
                parse_find_output, root='/var/userarchive',
            ),
            self._run_check(
                'Deployment server homedir', DEPLOY_HOSTS_QUERY,
                f'find "/home/{username}" -printf \'{FIND_PRINTF}\' 2>/dev/null; echo {EMPTY_OUTPUT_GUARD}',
                parse_find_output, root=f'/home/{username}',
            ),
            self._run_check(
                'HDFS', HDFS_HOSTS_QUERY,
                f'sudo -u hdfs kerberos-run-command hdfs hdfs dfs -ls -R /user/{username} 2>/dev/null; '
                f'echo {EMPTY_OUTPUT_GUARD}',
                parse_hdfs_ls_output, root=f'/user/{username}',
            ),
            self._run_check(
                'Hive warehouse', HDFS_HOSTS_QUERY,
                f'sudo -u hdfs kerberos-run-command hdfs hdfs dfs -ls /user/hive/warehouse 2>/dev/null '
                f'| grep -i "{username}"; echo {EMPTY_OUTPUT_GUARD}',
                parse_hdfs_ls_output, root='/user/hive/warehouse',
            ),
        ]

        report = {
            'username': username,
            'checks': self.checks,
        }

        for check in self.checks:
            if 'error' in check:
                logger.warning('%s: skipped (%s)', check['name'], check['error'])
                continue
            hosts_with_data = [host for host, data in check['results'].items() if data.get('found')]
            logger.info(
                '%s: %d/%d hosts checked, data found on: %s%s',
                check['name'], len(check['results']), check['hosts_checked'],
                ', '.join(sorted(hosts_with_data)) or 'none',
                f" (unreachable: {', '.join(check['unreachable_hosts'])})" if check['unreachable_hosts'] else '',
            )

        report_json = json.dumps(report, indent=2, sort_keys=True)
        if self.output:
            with open(self.output, 'w', encoding='utf-8') as report_file:
                report_file.write(report_json)
            logger.info('Full report written to %s', self.output)
        else:
            print(report_json)

        return 1 if any('error' in check for check in self.checks) else 0
