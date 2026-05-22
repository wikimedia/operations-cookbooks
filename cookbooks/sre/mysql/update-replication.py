"""MariaDB replication configuration cookbook.

The cookbooks configures replication from --master <master> to --replica <replica>.

It stops replication on the destination instance, runs the `CHANGE MASTER ...`
query and starts replication again.

No changes are made on <master>.
"""

import logging
import sys
import time
from argparse import ArgumentParser

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.mysql import MySQLCookbookRunnerBase, ensure
from spicerack.cookbook import CookbookBase
from spicerack.mysql import Instance as MInst
from wmflib.interactive import ensure_shell_is_durable

log = logging.getLogger(__name__)


def _run_query(dry_run: bool, ins: MInst, sql: str, args=(), log_query_args=True) -> dict:
    """Run query using pymysql not ssh sudo mysql"""
    if dry_run:
        if log_query_args and args:
            log.info(f"DRY-RUN: would have executed SQL: <{sql.strip()}> with args {args}")
        else:
            log.info(f"DRY-RUN: would have executed SQL: <{sql.strip()}>")
        return {}

    if log_query_args and args:
        log.info(f"Executing <{sql.strip()}> with args {args}")
    else:
        log.info(f"Executing <{sql.strip()}>")

    with ins.cursor() as (_conn, cur):
        _ = cur.execute(sql, args)
        row = cur.fetchone()
        _conn.commit()
        ins.check_warnings(cur)
        return dict(row) if row else {}


def _is_replica_status_ok(status: dict, repl_user: str, master_fqdn: str) -> bool:
    expected = {
        "Last_IO_Errno": 0,
        "Last_SQL_Errno": 0,
        "Master_Host": master_fqdn,
        "Master_Port": 3306,
        "Master_User": repl_user,
        "Slave_IO_Running": "Yes",
        "Slave_SQL_Running": "Yes",
    }
    is_ok = True
    for k, ev in expected.items():
        v = status.get(k)
        if v != ev:
            log.warning(f"Incorrect replica status: key '{k}' expected '{ev}' found '{v}'")
            is_ok = False

    if not is_ok:
        log.warning(status)

    return is_ok


def wait_for_replication_lag_to_lower(instance: MInst, timeout_s=100) -> None:
    t0 = time.monotonic()
    while True:
        ela = int(time.monotonic() - t0)
        if ela > timeout_s:
            log.error(f"[{ela}] Timing out")
            raise RuntimeError("Replication lag failed to lower")

        try:
            replag = int(instance.replication_lag())
        except Exception:
            log.info("Unable to extract replication lag", exc_info=True)
            time.sleep(10)
            continue

        if replag <= 1:
            log.info(f"[{ela}] Replication is healthy")
            return

        if (replag is None) or (replag > 1.0):
            log.info(f"[{ela}] Replication lag: {replag}s - waiting 10s to catch up")
            time.sleep(10)


class UpdateReplication(CookbookBase):
    argument_task_required = False
    argument_reason_required = False

    def argument_parser(self) -> ArgumentParser:
        """update-replication argument parser"""
        ap = super().argument_parser()
        ap.add_argument("--master", required=True, help="Master hostname e.g. db1234")
        ap.add_argument("--replica", required=True, help="Replica hostname")
        ap.add_argument("--port", default=3306, type=int, help="Master port")
        ap.add_argument("--delete-heartbeat", action="store_true", help="Remove entries from heartbeat table")
        ap.add_argument("--wait-lag-timeout", type=int, help="Wait for replication lag to lower with a timeout")
        return ap

    def get_runner(self, args):
        return UpdateReplicationRunner(args, self.spicerack)


class UpdateReplicationRunner(MySQLCookbookRunnerBase):
    def __init__(self, args, spicerack):
        self.args = args
        self.spicerack = spicerack

    @property
    def max_hosts(self) -> int:
        return 0

    @max_hosts.setter
    def max_hosts(self, val: int) -> None:
        raise AttributeError("max_hosts is read-only")

    @property
    def permitted_sections(self) -> list:
        return []

    @permitted_sections.setter
    def permitted_sections(self, val: list) -> None:
        raise AttributeError("permitted_sections is read-only")

    def run(self):
        ensure_shell_is_durable()

        repl_user, repl_password = self._load_replication_user_password()

        master = self.get_minst(self.args.master)
        master_fqdn = str(master.host)
        assert "." in master_fqdn, f"{master_fqdn} has no dots"
        replica = self.get_minst(self.args.replica)

        ensure(master_fqdn != str(replica.host), "Master and replica cannot be the same host")

        dry_run = self.spicerack.dry_run
        _run_query(dry_run, replica, "STOP REPLICA")

        _run_query(dry_run, replica, "CHANGE MASTER TO MASTER_USE_GTID = no")

        sql = """
        CHANGE MASTER TO
            MASTER_HOST=%(master_fqdn)s,
            MASTER_USER=%(user)s,
            MASTER_PASSWORD=%(password)s,
            MASTER_PORT=%(port)s,
            MASTER_SSL=1
        """
        _run_query(
            dry_run,
            replica,
            sql,
            dict(master_fqdn=master_fqdn, password=repl_password, port=self.args.port, user=repl_user),
            log_query_args=False,
        )

        _run_query(dry_run, replica, "CHANGE MASTER TO MASTER_USE_GTID = slave_pos")

        if self.args.delete_heartbeat:
            _run_query(dry_run, replica, "DELETE FROM heartbeat.heartbeat;")

        _run_query(dry_run, replica, "START REPLICA")

        if dry_run:
            return  # Next steps are only useful in real runs

        time.sleep(2)  # usually enough to start repl
        for attempt in range(1, 11):
            log.info("[%s/10] checking replica status", attempt)
            status: dict = replica.show_slave_status()
            replica_ok = _is_replica_status_ok(status, repl_user, master_fqdn)
            if replica_ok:
                log.info("Replica status is OK")
                break

            log.info("[%s/10] checking replica status again in 5s", attempt)
            time.sleep(5)

        else:
            log.error("Unexpected replica status")
            sys.exit(1)

        # Trim heartbeat again in case there were stale heartbeat in the backlog
        if self.args.delete_heartbeat:
            _run_query(dry_run, replica, "DELETE FROM heartbeat.heartbeat;")

        if self.args.task_id:
            phab = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            reason = self.spicerack.admin_reason(f"Set {self.args.replica} to replicate from {self.args.master}")
            phab.task_comment(self.args.task_id, reason.reason)

        if self.args.wait_lag_timeout is not None:
            time.sleep(2)  # wait 2 sec to be sure we are measuring the real, current lag
            wait_for_replication_lag_to_lower(replica, self.args.wait_lag_timeout)
