# NOTE: this scripts is written defensively. Please prioritize safety and readability,
# minimize abstractions and state, enable type checking, do assertions, write tests
# pylint: disable=missing-docstring
# pylint: disable=R0913,R0917
# flake8: noqa: D103

# NOTE: For this initial iteration we expect the target host to be a new host

# TODO: add cluster-wide soft locking
# TODO: add instance-level locking


from argparse import ArgumentParser
from contextlib import contextmanager
from datetime import timedelta
from typing import Tuple, Dict, Generator
import logging
import re
import sys
import time

from pymysql.cursors import DictCursor

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.dbctl import Dbctl
from spicerack import Spicerack
from spicerack.mysql import Instance
from spicerack.remote import Remote, RemoteHosts
from transferpy.Transferer import Transferer
from wmflib.config import load_yaml_config
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable, ask_confirmation
import transferpy.transfer

log = logging.getLogger(__name__)

SLUG = "cookbooks.sre.mysql.clone"


# General utility functions to be moved to a shared module later on


def ensure(condition: bool, msg: str) -> None:
    # just some syntactic sugar for readability
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def step(slug: str, msg: str) -> None:
    """
    Log next step in a friendly/greppable format.
    """
    # TODO: store the step in zarcillo/etcd to create visibility
    # around the automation process
    # TODO: also log msg in open telemetry format for tracing
    log.info("[%s.%s] %s", SLUG, slug, msg)


def parse_phabricator_task(t: str) -> int:
    ensure(t.startswith("T"), f"Incorrect Phabricator task ID {t}")
    return int(t.lstrip("T"))


def parse_db_host_fqdn(hn: str) -> Tuple[str, str]:
    """
    Return short hostname and validated FQDN
    """
    hn = hn.strip()
    chunks = hn.split(".")
    ensure(len(chunks) == 3, f"Incorrect FQDN {hn}")
    hostname, dcname, tld = chunks
    ensure(tld == "wmnet", f"Invalid TLD `{tld}`")
    if dcname == "codfw":
        ensure(hostname.startswith("db2"), "Inconsistent FQDN")
    elif dcname == "eqiad":
        ensure(hostname.startswith("db1"), "Inconsistent FQDN")
    else:
        raise ValueError(f"Incorrect datacenter {hn}")
    return (hostname, hn)


def get_db_instance(spicerack: Spicerack, fqdn: str) -> Instance:
    parse_db_host_fqdn(fqdn)
    db = spicerack.mysql().get_dbs(fqdn)
    return db.list_hosts_instances()[0]


def connect_to_zarcillo(spicerack: Spicerack) -> Instance:
    return get_db_instance(spicerack, "db1215.eqiad.wmnet")  # hardcoded intentionally


def query_zarcillo_one_row(spicerack: Spicerack, sql: str, params: Tuple) -> Dict:
    instance = connect_to_zarcillo(spicerack)
    return instance.fetch_one_row(sql, params, database="zarcillo")


def gen_grafana_mysql_url(fqdn: str) -> str:
    hn, _ = parse_db_host_fqdn(fqdn)
    # pylint: disable=C0301
    return f"https://grafana-rw.wikimedia.org/d/000000273/mysql?forceLogin&from=now-3h&orgId=1&refresh=1m&to=now&var-job=All&var-port&var-server={hn}"


def check_db_role_on_zarcillo(spicerack: Spicerack, fqdn: str, expect_db_is_master=False) -> str:
    """
    Check the DB role on zarcillo and return the section
    """
    sql = "SELECT name, server, port, group FROM instances WHERE server = %s"
    row = query_zarcillo_one_row(spicerack, sql, (fqdn,))
    ensure(row["src_port"] == 3306, "Source is not on port 3306")
    name = row["name"]

    hn, _ = parse_db_host_fqdn(fqdn)
    sql = "SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s"
    row = query_zarcillo_one_row(spicerack, sql, (hn,))
    if expect_db_is_master:
        ensure(row["cnt"] == 1, f"{fqdn} is not master according to zarcillo")
    else:
        ensure(row["cnt"] == 0, f"{fqdn} is master according to zarcillo")

    sql = "SELECT section FROM section_instances WHERE instance = %s"
    section_row = query_zarcillo_one_row(spicerack, sql, name)
    return section_row["section"]


def ensure_db_not_in_zacillo(spicerack: Spicerack, fqdn: str, hostname: str):
    sql = "SELECT COUNT(*) AS cnt FROM instances WHERE server = %s"
    r = query_zarcillo_one_row(spicerack, sql, (fqdn,))
    ensure(r["cnt"] == 0, f"{fqdn} found in instances table on zarcillo")

    sql = "SELECT COUNT(*) AS cnt FROM section_instances WHERE instance = %s"
    r = query_zarcillo_one_row(spicerack, sql, (hostname,))
    ensure(r["cnt"] == 0, f"{hostname} found in section_instances table on zarcillo")

    sql = "SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s"
    r = query_zarcillo_one_row(spicerack, sql, (hostname,))
    ensure(r["cnt"] == 0, f"{hostname} found in masters table on zarcillo")


def _fetch_db_remotehost(remote: Remote, fqdn: str) -> RemoteHosts:
    parse_db_host_fqdn(fqdn)
    query = "P{" + fqdn + "} and A:db-all and not A:db-multiinstance"
    h = remote.query(query)
    if len(h.hosts) != 1:
        print(f"No suitable host matching {fqdn} have been found, exiting")
        raise RuntimeError
    return _extract_first_node(h)


def _extract_first_node(h: RemoteHosts):
    # NOTE: RemoteHosts is a cluster with only one host in it...
    return list(h.hosts)[0]


def _parse_replication_status(replication_status) -> Tuple[str, int]:
    # TODO: use XML format?
    binlog_file_matches = re.findall(r"\sMaster_Log_File:\s*(\S+)", replication_status)
    repl_position_matches = re.findall(r"\sExec_Master_Log_Pos:\s*(\d+)", replication_status)

    if len(binlog_file_matches) != 1 or len(repl_position_matches) != 1:
        log.error("Could not find the replication position, aborting")
        raise AbortError

    binlog_fn: str = str(binlog_file_matches[0])
    position: int = int(repl_position_matches[0])
    return binlog_fn, position


def _fetch_replication_status(host: RemoteHosts) -> Tuple[str, int]:
    replication_status = host.run_sync('mysql -e "SHOW SLAVE STATUS\\G"')
    replication_status = list(replication_status)[0][1].message().decode("utf-8")

    return _parse_replication_status(replication_status)


@contextmanager
def transaction(ins: Instance, dbname: str) -> Generator[DictCursor, None, None]:
    """Transaction wrapper:

    >>> z_inst = connect_to_zarcillo(sr)
    >>> with transaction(z_inst, "zarcillo") as tx:
    >>>     tx.execute("SELECT 1", ())
    """
    with ins.cursor(database=dbname) as (dbconn, cur):
        try:
            dbconn.begin()
            yield cur
            dbconn.commit()
        except Exception as e:  # pylint: disable=W0718
            dbconn.rollback()
            log.error("Transaction rolled back due to: %s", e)
        finally:
            ins.check_warnings(cur)
            cur.close()


def _add_host_to_zarcillo(
    spicerack: Spicerack, hostname: str, fqdn: str, datacenter: str, rack: str, section: str
) -> None:
    z_inst = connect_to_zarcillo(spicerack)
    with transaction(z_inst, "zarcillo") as tx:
        sql = """SET SESSION binlog_format=ROW;
            INSERT INTO instances (name, server, port, `group`) \
            VALUES (%s, %s, 3306, 'core')"""
        tx.execute(sql, (hostname, fqdn))

        sql = """SET SESSION binlog_format=ROW;
            INSERT INTO section_instances (instance, section) VALUES (%s,%s)"""
        tx.execute(sql, (hostname, section))

        sql = """SET SESSION binlog_format=ROW;
            INSERT INTO servers (fqdn, hostname, dc, rack) VALUES (%s, %s, %s, %s)"""
        tx.execute(sql, (fqdn, hostname, datacenter, rack))


def pool_in_instance_slowly(dbctl: Dbctl, fqdn: str, hostname: str, phabricator_task_id: int) -> None:
    """Pool in with 10 min intervals (synchronous)"""
    for perc in (2, 5, 10, 20, 50, 75, 100):
        step("repool", f"Pooling in {fqdn} at {perc}%")
        dbctl.instance.pool(hostname, percentage=perc)
        dbctl.config.commit(batch=False, comment=f"Pool {hostname} T{phabricator_task_id}")
        step("wait", "Waiting 10m")
        time.sleep(60 * 10)


def check_pooling_status(
    dbctl: Dbctl,
    source_hostname: str,
    source_fqdn: str,
    expect_pooled=True,
    reject_candidate_master=True,
) -> str:
    """
    Check pooling status, extract section, reject unsuitable instances
    """
    sections = dbctl.instance.get(source_hostname).sections
    ensure(len(sections) == 1, f"{source_fqdn} has {sections.keys()} sections")
    sec_name: str = sections.keys()[0]
    sd: Dict = sections[sec_name]
    if expect_pooled:
        ensure(sd["pooled"], f"{source_fqdn} is not pooled")
    else:
        ensure(not sd["pooled"], f"{source_fqdn} is pooled")

    if reject_candidate_master:
        ensure(not sd["candidate_master"], f"{source_fqdn} is candidate master")

    return sec_name


class CloneMySQL(CookbookBase):
    """Clone one MySQL host into another.

    This script depools a source and destination host, clone data into the destination host,
    checks replicaton health, repools both.
    Run the script on cumin under screen/tmux/byobu
    This tool is for internal use for the DBA team. It needs to run as root.
    """

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("--source", help="Cumin query to match the host of the source.")
        parser.add_argument("--target", help="Cumin query to match the host of the target.")
        parser.add_argument("--primary", help="Cumin query to match the host of the primary.")
        parser.add_argument("--task", help="Phabricator task")
        parser.add_argument("--nopool", action="store_true", help="Do not pool in target host")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return CloneMySQLRunner(args, self.spicerack)


class CloneMySQLRunner(CookbookRunnerBase):
    """Clone MySQL cookbook runner."""

    def __init__(self, args, spicerack):
        """Clone one MySQL host into another."""
        if sys.flags.optimize:
            print("Running python with -O is not supported")
            sys.exit(1)
        ensure_shell_is_durable()

        self.alerting_hosts = spicerack.alerting_hosts
        self.admin_reason = spicerack.admin_reason("MySQL Clone")
        self.remote = spicerack.remote()

        self.source_hostname, self.source_fqdn = parse_db_host_fqdn(args.source)
        self.source_host = _fetch_db_remotehost(self.remote, self.source_fqdn)

        self.target_hostname, self.target_fqdn = parse_db_host_fqdn(args.target)
        self.target_host = _fetch_db_remotehost(self.remote, self.target_fqdn)

        self.primary_hostname, self.primary_fqdn = parse_db_host_fqdn(args.primary)
        self.primary_host = _fetch_db_remotehost(self.remote, self.primary_fqdn)

        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)
        # Other prep
        self.tp_options = dict(transferpy.transfer.parse_configurations(transferpy.transfer.CONFIG_FILE))
        # this also handles string->bool conversion where necessary
        self.tp_options = transferpy.transfer.assign_default_options(self.tp_options)
        # If source and target are in different dcs, encrypt
        netbox_source = spicerack.netbox_server(str(self.source_host).split(".", maxsplit=1)[0])
        netbox_target = spicerack.netbox_server(str(self.target_host).split(".", maxsplit=1)[0])
        if netbox_source.as_dict()["site"]["slug"] != netbox_target.as_dict()["site"]["slug"]:
            self.tp_options["encrypt"] = True

        config = load_yaml_config(spicerack.config_dir / "mysql" / "config.yaml")
        self.replication_user = config["replication_user"]
        self.replication_password = config["replication_password"]

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"of {self.source_host} onto {self.target_host}"

    def run(self):
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)
        hosts_to_downtime = [
            list(self.source_host.hosts)[0],
            list(self.target_host.hosts)[0],
        ]
        self.alerting_hosts(hosts_to_downtime).downtime(self.admin_reason, duration=timedelta(hours=48))
        self._run_clone()

    def _run_clone(self):
        self.logger.info("Stopping mariadb on %s", self.source_host)

        self._run_scripts(self.source_host, ['mysql -e "STOP SLAVE;"'])
        # Sleep for a second to make sure the position is updated
        time.sleep(1)

        binlog_file, repl_position = _fetch_replication_status(self.source_host)
        self._run_scripts(self.source_host, ["service mariadb stop"])

        self._run_scripts(
            self.target_host,
            ['mysql -e "STOP SLAVE;"', "service mariadb stop", "rm -rf /srv/sqldata/"],
        )

        t = Transferer(
            str(self.source_host),
            "/srv/sqldata",
            [str(self.target_host)],
            ["/srv/"],
            self.tp_options,
        )
        # transfer.py produces a lot of log chatter, cf T330882
        self.logger.debug("Starting transferpy, expect cumin errors")
        r = t.run()
        self.logger.debug("Transferpy complete")
        if r[0] != 0:
            raise RuntimeError("Transfer failed")

        scripts = [
            "chown -R mysql. /srv/*",
            'systemctl set-environment MYSQLD_OPTS="--skip-slave-start"',
            "systemctl start mariadb",
            'mysql -e "STOP SLAVE; RESET SLAVE ALL"',
        ]
        self._run_scripts(self.target_host, scripts)

        sql = (
            f"CHANGE MASTER TO master_host='{self.primary_host}', "
            f"master_port=3306, master_ssl=1, master_log_file='{binlog_file}', "
            f"master_log_pos={repl_position}, master_user='{self.replication_user}', "
            f"master_password='{self.replication_password}';"
        )
        sql = sql.replace('"', '\\"')
        scripts = [
            f'mysql -e "{sql}"',
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.target_host, scripts)

        self._wait_for_replication(self.target_host)

        scripts = [
            'mysql -e "STOP SLAVE;"',
            "mysql_upgrade --force",
            'mysql -e "CHANGE MASTER TO MASTER_USE_GTID=Slave_pos;"',
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.target_host, scripts)
        scripts = [
            "systemctl start mariadb",
            "systemctl restart mariadb",
            'mysql -e "START SLAVE;"',
        ]
        self._run_scripts(self.source_host, scripts)

    def _run_scripts(self, host, scripts) -> None:
        for script in scripts:
            try:
                confirm_on_failure(host.run_sync, script)
            except AbortError:
                self.logger.error("%s: execution aborted", script)
                raise

    def _wait_for_replication(self, host) -> None:
        replag = 1000.0
        while replag > 1.0:
            replag = self._get_replication(host)
            if (replag is None) or (replag > 1.0):
                print("Waiting for replag to catch up")
                time.sleep(60)

    def _get_replication(self, host) -> float:
        query = """
        SELECT greatest(0, TIMESTAMPDIFF(MICROSECOND, max(ts), UTC_TIMESTAMP(6)) - 500000)/1000000
        FROM heartbeat.heartbeat
        ORDER BY ts LIMIT 1;
        """
        query = query.replace("\n", "")
        query_res = host.run_sync(f'mysql -e "{query}"')
        query_res = list(query_res)[0][1].message().decode("utf-8")
        replag = 1000.0
        for line in query_res.split("\n"):
            if not line.strip():
                continue
            count = line.strip()
            try:
                count = float(count)
            except ValueError:
                continue
            replag = count
        return replag
