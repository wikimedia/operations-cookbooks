"""Pool or depool a DB from dbctl."""

from datetime import datetime, timedelta
from pprint import pformat
from time import sleep
from typing import Dict, Generator, Tuple
import logging

from conftool.extensions.dbconfig.action import ActionResult
from conftool.extensions.dbconfig.entities import Instance as DBCInst
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.decorators import retry
from spicerack import Spicerack
from spicerack.mysql import Instance as MInst
from spicerack.remote import Remote, RemoteHosts
from wmflib.interactive import ensure_shell_is_durable, ask_confirmation

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)

# TODO: improve handling of spurious changes, right now it bails out
# TODO: check that the host is not downtimed and green in Icinga/AM?
# TODO: check for mysql/metrics errors during the repooling operation?
# TODO: support both fqdn or hostname as CLI argument


def ensure(condition: bool, msg: str) -> None:
    """Just some syntactic sugar for readability."""
    if condition:
        return
    logger.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def _fetch_mysql_instance_wildcard(spicerack: Spicerack, hostname: str) -> MInst:
    ensure("." not in hostname, f"Invalid hostname: contains dot '{hostname}'")
    db = spicerack.mysql().get_dbs(f"{hostname}.*")
    instances = db.list_hosts_instances()
    ensure(len(instances) == 1, f"{len(instances)} found, expected one")
    return instances[0]


def _fetch_db_remotehost(remote: Remote, fqdn: str) -> RemoteHosts:
    query = "P{" + fqdn + "} and A:db-all and not A:db-multiinstance"
    h = remote.query(query)
    ensure(len(h.hosts) == 1, f"{len(h.hosts)} hosts matching '{fqdn}'")
    return h


def _get_fqdn(mi: MInst) -> str:
    t = tuple(mi.host.hosts)
    ensure(len(t) == 1, f"{len(t)} hosts in {mi}")
    return t[0]


def _run_cmd(host: RemoteHosts, cmd: str, is_safe=False) -> str:
    out = host.run_sync(cmd, is_safe=is_safe, print_progress_bars=False, print_output=False)
    return list(out)[0][1].message().decode("utf-8")


def _count_tcp_connections_port_3306(remote_host: RemoteHosts) -> int:
    # TODO: this breaks on multiinstance hosts or if there are any other unexpected conns
    cmd = """ss -ntH state all exclude listening '( dport = :3306 or sport = :3306 )' """
    out = _run_cmd(remote_host, cmd, is_safe=True)
    conn_cnt = out.count("\n")
    return conn_cnt


def _fetch_replication_delay_ms(ins: MInst) -> int:
    sql = """
    SELECT TIMESTAMPDIFF(MICROSECOND, max(ts), UTC_TIMESTAMP(6)) AS delta_us
    FROM heartbeat.heartbeat ORDER BY ts LIMIT 1 """
    r = ins.fetch_one_row(sql)
    return int(r["delta_us"] / 1_000)


def _fetchall(ins: MInst, sql: str, args: tuple) -> Tuple[Dict]:
    with ins.cursor() as (_conn, cur):
        _ = cur.execute(sql, args)
        res = tuple(cur.fetchall())
        ins.check_warnings(cur)
        return res


def _fetch_instance_connections_count_detailed(ins: MInst) -> Tuple[Dict]:
    """Gather database instance connection counts.

    +----------+-----------------+-----------+
    | count(*) | user            | command   |
    +----------+-----------------+-----------+
    |        1 | cumin2024       | Query     |
    |        1 | event_scheduler | Daemon    |
    |        3 | orchestrator    | Sleep     |
    |        1 | system user     | Slave_IO  |
    |        1 | system user     | Slave_SQL |
    |       27 | wikiuser2023    | Sleep     |
    +----------+-----------------+-----------+
    """
    sql = """SELECT user, command, COUNT(*) AS cnt
        FROM information_schema.processlist GROUP BY user, command"""
    return _fetchall(ins, sql, ())


def _fetch_instance_connections_count_wikiusers(ins: MInst) -> int:
    """Count database instance connections matching wiki-related users."""
    sql = "SELECT COUNT(*) AS cnt FROM information_schema.processlist WHERE user LIKE '%%wiki%%'"
    return ins.fetch_one_row(sql, ())["cnt"]


def _fetch_instance_by_name(dbctl, hostname: str) -> DBCInst:
    dbi = dbctl.instance.get(hostname)
    ensure(dbi is not None, f"Unable to find instance {hostname} in dbctl. Aborting.")
    return dbi


def _gather_instance_status(host: RemoteHosts, mi: MInst) -> int:
    socket_cnt = _count_tcp_connections_port_3306(host)
    wikiuser_cnt = _fetch_instance_connections_count_wikiusers(mi)
    delay_ms = _fetch_replication_delay_ms(mi)
    msg = "Replication delay: %7.3f s Wikiuser conn cnt: %3d TCP socket 3306 cnt: %3d"
    logger.info(msg, delay_ms / 1000.0, wikiuser_cnt, socket_cnt)
    return wikiuser_cnt


# This class is also used as a base class for the Depool cookbook
class Pool(CookbookBase):
    """Pool a DB instance in dbctl and allow to gradually increase its pooled percentage.

    There are three available profiles to control the repool steps. All of them use a power of two progression for
    increasing the percentage from 0% to 100%.

    The default profile does it in 4 steps. There are also a fast profile with just 2 steps and a slow one with 10
    steps.

    The current sleep between steps is 15 minutes.

    Examples:
        # Pool the instance gradually sleeping in between steps
        sre.mysql.pool -r "Some reason" db1001

        # Pool the instance and update a Phabricator task at the start and end of the pooling operation
        sre.mysql.pool -r "Some reason" -t T12345 db1001

        # Pool the instance quickly with just two steps
        sre.mysql.pool -r "Some reason" --fast db1001

    """

    def argument_parser(self):
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "-r",
            "--reason",
            required=True,
            help="The administrative reason for the action.",
        )
        parser.add_argument(
            "-t",
            "--task-id",
            help="The Phabricator task ID to update and refer (i.e.: T12345)",
        )
        if self.__class__.__name__ == "Pool":
            profile = parser.add_mutually_exclusive_group()
            profile.add_argument(
                "--fast",
                action="store_true",
                help="Repool the host quicker with just two steps.",
            )
            profile.add_argument(
                "--slow",
                action="store_true",
                help="Repool the host more slowly, with ten steps.",
            )

        # TODO: add support for multiple instances? Based on what? (puppetdb, dbctl, orchestrator)
        parser.add_argument("instance", help="Instance name as defined in dbctl.")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        args.operation = self.__class__.__name__.lower()
        return PoolDepoolRunner(args, self.spicerack)


class PoolDepoolRunner(CookbookRunnerBase):
    """Pool or depool a MySQL instance cookbook runner."""

    def __init__(self, args, spicerack):
        """As specified by Spicerack API."""
        # Silence some more noisy loggers for the dry-run mode
        logging.getLogger("etcd.client").setLevel(logging.INFO)
        logging.getLogger("conftool").setLevel(logging.INFO)

        ensure_shell_is_durable()

        self.args = args
        self.pool = args.operation == "pool"
        self.dbctl = spicerack.dbctl()
        self.reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
        self.dry_run = spicerack.dry_run
        self._mysql = spicerack.mysql()

        if self.pool:
            if self.args.slow:
                self.steps: tuple[int, ...] = (
                    1,
                    4,
                    9,
                    16,
                    25,
                    36,
                    49,
                    64,
                    81,
                    100,
                )  # 10 steps, power or 2 progression
            elif self.args.fast:
                self.steps = (25, 100)  # 2 steps, power of 2 progression
            else:
                self.steps = (6, 25, 56, 100)  # 4 steps, power of 2 progression

        dbi: DBCInst = _fetch_instance_by_name(self.dbctl, args.instance)
        self._mysql_instance: MInst = _fetch_mysql_instance_wildcard(spicerack, dbi.name)
        fqdn = _get_fqdn(self._mysql_instance)

        self.datacenter = dbi.tags.get("datacenter")
        self.remote_host = _fetch_db_remotehost(spicerack.remote(), fqdn)

        if self.reason.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        suffix = ""
        if self.pool:
            adj = "slowly" if self.args.slow else "quickly" if self.args.fast else "gradually"
            suffix = f" {adj} with {len(self.steps)} steps"
        return f"{self.args.instance}{suffix} - {self.reason.reason}"

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-instance."""
        # TTL includes both the sleep time (900s) plus the potential retries for wait_diff_clean (30*30s) for each step
        ttl = 1800 * len(self.steps) if self.pool else 60
        return LockArgs(suffix=self.args.instance, concurrency=1, ttl=ttl)

    def check_action_result(self, action_result: ActionResult, message: str) -> None:
        """Raise on failure and log any messages present in an ActionResult instance."""
        for result_message in action_result.messages:
            logger.log(logging.INFO if action_result.success else logging.ERROR, result_message)

        if action_result.announce_message:
            logger.info(action_result.announce_message)

        if not action_result.success:
            raise RuntimeError(f"Failed to {message}")

    def run(self) -> None:
        """As required by the Spicerack API."""
        if self.pool:
            if self.phabricator is not None:
                msg = f"Start pool of {self.runtime_description} - {self.reason.owner}"
                self.phabricator.task_comment(self.reason.task_id, msg)

            self.gradual_pooling()

        else:
            msg = "depool instance {self.args.instance}"
            self.wait_diff_clean()
            ret = self.dbctl.instance.depool(self.args.instance)
            self.check_action_result(ret, msg)
            self.commit_change(msg)

            self.wait_for_connection_drain()

        if self.phabricator is not None:
            msg = f"Completed {self.args.operation} of {self.runtime_description} - {self.reason.owner}"
            self.phabricator.task_comment(self.reason.task_id, msg)

    def _fetch_current_pooling(self, i: str, percentage: int) -> set:
        instance = self.dbctl.instance.get(i)
        current_pooling = {
            (section["pooled"], section["percentage"] >= percentage) for section in instance.sections.values()
        }
        return current_pooling

    def gradual_pooling(self) -> None:
        """Gradually pool the instance with increasing percentages."""
        sleep_duration = 5 if self.dry_run else 900
        for percentage in self.steps:
            current_pooling = self._fetch_current_pooling(self.args.instance, percentage)
            # Skip if all the sections are pooled with a percentage equal or greater than the percentage to set
            if len(current_pooling) == 1 and current_pooling.pop() == (True, True):
                msg = "Skipping pooling instance %s at %d%%: instance already pooled with higher percentage"
                logger.info(msg, self.args.instance, percentage)
                continue

            msg = f"Pooling instance {self.args.instance} at {percentage}%"
            logger.info(msg)
            self.wait_diff_clean()
            ret = self.dbctl.instance.pool(self.args.instance, percentage=percentage)
            self.check_action_result(ret, msg)
            self.commit_change(msg)
            if percentage == 100:
                logger.debug("pooling-in completed")
                return

            monitor_end_t = datetime.utcnow() + timedelta(seconds=sleep_duration)
            logger.info("Next pool-in step will happen after %s", monitor_end_t)
            while datetime.utcnow() < monitor_end_t:
                _gather_instance_status(self.remote_host, self._mysql_instance)
                sleep(10)

    def commit_change(self, message: str) -> None:
        """Check the diff and commit the changepy."""
        ret, diff = self.get_diff()
        self.check_action_result(ret, f"get diff to {message}")

        self.check_diff(diff)

        ret = self.dbctl.config.commit(batch=True, datacenter=self.datacenter, comment=self.reason.reason)
        self.check_action_result(ret, f"commit change to {message}")

    @retry(
        tries=30,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        failure_message="Waiting for dbctl config diff to be clean",
        exceptions=(RuntimeError,),
    )
    def wait_diff_clean(self) -> None:
        """Poll until dbctl config diff is clean."""
        ret, _ = self.get_diff()
        if ret.success and ret.exit_code == 0:  # Empty diff
            return

        raise RuntimeError("dbctl config has a pending diff or unable to get the diff")

    def wait_for_connection_drain(self) -> None:
        """Wait for connections from the parser to drain.

        NOTE: this does not support misc databases
        """
        timeout = datetime.utcnow() + timedelta(hours=1)
        logger.info("Monitoring number of wikiuser* connections and sockets on port 3306")
        while datetime.utcnow() < timeout:
            wikiuser_cnt = _gather_instance_status(self.remote_host, self._mysql_instance)
            if wikiuser_cnt == 0:
                logger.info("Connection drain completed")
                return

            sleep(10)

        d = _fetch_instance_connections_count_detailed(self._mysql_instance)
        logger.info("Drain timeout! Connection summary: %r", d)
        raise RuntimeError("The instance failed to drain in an hour")

    def get_diff(self) -> tuple[ActionResult, Generator]:
        """Get the current dbctl config diff."""
        ret, diff = self.dbctl.config.diff(datacenter=self.datacenter, force_unified=True)
        self.check_action_result(ret, "evaluate dbctl config diff")
        return ret, diff

    def check_diff(self, diff: Generator) -> None:
        """Ensure that the diff has only the expected change in it."""
        # Count the diff lines unrelated to the current change
        count = 0
        for line in diff:
            if (
                # ignore control lines and context lines
                any(line.startswith(prefix) for prefix in (" ", "---", "+++", "@@"))
                # ignore DB groups lines (e.g. '"vslow": {', '}')
                or any(substr in line for substr in ("{", "}"))
            ):
                continue

            if self.args.instance not in line:
                count += 1

        if count:
            logger.info("CAUTION: The current diff has unexpected spurious changes:\n%s", pformat(diff))
            ask_confirmation("Review the changes. Do you still want to commit them?")
