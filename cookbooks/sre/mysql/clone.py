# NOTE: this scripts is written defensively. Please prioritize safety and readability,
# minimize abstractions and state, enable type checking, do assertions, write tests
# pylint: disable=missing-docstring
# pylint: disable=R0913,R0917
# flake8: noqa: D103

# NOTE: For this initial iteration we expect the target host to be a new host

# TODO: add cluster-wide soft locking
# TODO: add instance-level locking

# TODO: detect if target is moving to a new section and ask for confirmation
# TODO: upsert entries in zarcillo
# TODO: allow setting non-core group in zarcillo

import logging
import re
import sys
import time
from argparse import ArgumentParser
from contextlib import contextmanager
from datetime import timedelta
from logging import Logger
from typing import Tuple, Dict, Generator, List

import transferpy.transfer
from pymysql.cursors import DictCursor
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.dbctl import Dbctl
from spicerack.decorators import retry
from spicerack.mysql import Instance as MInst, Mysql
from spicerack.remote import Remote, RemoteHosts, RemoteError
from transferpy.Transferer import Transferer
from wmflib.config import load_yaml_config
from wmflib.interactive import AbortError, confirm_on_failure, ensure_shell_is_durable, ask_confirmation

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


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
    ensure(len(t) > 1, "Phabricator task ID is required")
    ensure(t.startswith("T"), f"Incorrect Phabricator task ID {t}")
    return int(t.lstrip("T"))


def parse_db_host_fqdn(fqdn: str) -> Tuple[str, str]:
    """
    Return short hostname and validated FQDN
    """
    fqdn = fqdn.strip()
    chunks = fqdn.split(".")
    ensure(len(chunks) == 3, f"Incorrect FQDN '{fqdn}', expecting <hn>.<dc>.wmnet")
    hostname, dcname, tld = chunks
    ensure(tld == "wmnet", f"Invalid TLD `{tld}`")
    if hostname.startswith("db1"):
        ensure(dcname == "eqiad", f"Inconsistent FQDN '{fqdn}'")
    if hostname.startswith("db2"):
        ensure(dcname == "codfw", f"Inconsistent FQDN '{fqdn}'")

    return (hostname, fqdn)


def get_db_instance(mysql: Mysql, fqdn: str) -> MInst:
    parse_db_host_fqdn(fqdn)
    db = mysql.get_dbs(fqdn)
    return db.list_hosts_instances()[0]


def connect_to_zarcillo(mysql: Mysql) -> MInst:
    return get_db_instance(mysql, "db1215.eqiad.wmnet")  # hardcoded intentionally


def query_zarcillo_one_row(mysql: Mysql, sql: str, params: Tuple) -> Dict:
    instance = connect_to_zarcillo(mysql)
    return instance.fetch_one_row(sql, params, database="zarcillo")


def gen_grafana_mysql_url(hn: str) -> str:
    # uses short hostname
    return f"https://grafana.wikimedia.org/d/000000273/mysql?orgId=1&var-job=All&var-server={hn}"


def check_db_role_on_zarcillo(mysql: Mysql, fqdn: str, expect_db_is_master: bool = False) -> str:
    """
    Check the DB role on zarcillo and return the section
    """
    sql = "SELECT name, server, port, `group` FROM instances WHERE server = %s"
    row = query_zarcillo_one_row(mysql, sql, (fqdn,))
    ensure(row["port"] == 3306, "Source is not on port 3306")
    name = row["name"]

    hn, _ = parse_db_host_fqdn(fqdn)
    sql = "SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s"
    row = query_zarcillo_one_row(mysql, sql, (hn,))
    if expect_db_is_master:
        ensure(row["cnt"] == 1, f"{fqdn} is not master according to zarcillo")
    else:
        ensure(row["cnt"] == 0, f"{fqdn} is master according to zarcillo")

    sql = "SELECT section FROM section_instances WHERE instance = %s"
    section_row = query_zarcillo_one_row(mysql, sql, name)
    return section_row["section"]


def ensure_db_not_in_zacillo(mysql: Mysql, fqdn: str, hostname: str) -> None:
    sql = "SELECT COUNT(*) AS cnt FROM instances WHERE server = %s"
    r = query_zarcillo_one_row(mysql, sql, (fqdn,))
    ensure(r["cnt"] == 0, f"{fqdn} found in instances table on zarcillo")

    sql = "SELECT COUNT(*) AS cnt FROM section_instances WHERE instance = %s"
    r = query_zarcillo_one_row(mysql, sql, (hostname,))
    ensure(r["cnt"] == 0, f"{hostname} found in section_instances table on zarcillo")

    sql = "SELECT COUNT(*) AS cnt FROM masters WHERE instance = %s"
    r = query_zarcillo_one_row(mysql, sql, (hostname,))
    ensure(r["cnt"] == 0, f"{hostname} found in masters table on zarcillo")


# occasional "spicerack.remote.RemoteError: No hosts provided" has been raised
@retry(tries=20, delay=timedelta(seconds=5), backoff_mode="constant", exceptions=(RemoteError,))
def _remotehosts_query(remote: Remote, query, fqdn: str) -> RemoteHosts:
    h = remote.query(query)
    if len(h.hosts) != 1:
        print(f"No suitable host matching {fqdn} have been found")
        raise RuntimeError
    return h


def _fetch_db_remotehost(remote: Remote, fqdn: str) -> RemoteHosts:
    parse_db_host_fqdn(fqdn)
    query = "A:db-all and not A:db-multiinstance and P{%s}" % fqdn
    log.debug("Searching remote '%s'", query)
    ensure(len(fqdn) > 0, "Empty fqdn in _fetch_db_remotehost")
    return _remotehosts_query(remote, query, fqdn)


def _parse_replication_status(replication_status: str) -> Tuple[str, int]:
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
    replication_status = _run(host, 'mysql -e "SHOW SLAVE STATUS\\G"', is_safe=True)
    return _parse_replication_status(replication_status)


def _run(host: RemoteHosts, cmd: str, is_safe: bool = False) -> str:
    out = confirm_on_failure(host.run_sync, cmd, is_safe=is_safe, print_progress_bars=False, print_output=True)
    try:
        return list(out)[0][1].message().decode("utf-8")
    except (IndexError, TypeError):
        return ""


@contextmanager
def transaction(ins: MInst, dbname: str) -> Generator[DictCursor, None, None]:
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


def _add_host_to_zarcillo(mysql: Mysql, hostname: str, fqdn: str, datacenter: str, rack: str, section: str) -> None:
    z_inst = connect_to_zarcillo(mysql)

    # TODO: delete previous entries / upsert
    with z_inst.cursor() as (_conn, cursor):
        cursor.execute("SET SESSION binlog_format=ROW;")

    with transaction(z_inst, "zarcillo") as tx:

        sql = """INSERT INTO instances (name, server, port, `group`) VALUES (%s, %s, 3306, 'core')"""
        tx.execute(sql, (hostname, fqdn))

        sql = """INSERT INTO section_instances (instance, section) VALUES (%s,%s)"""
        tx.execute(sql, (hostname, section))

        sql = """INSERT INTO servers (fqdn, hostname, dc, rack) VALUES (%s, %s, %s, %s)"""
        tx.execute(sql, (fqdn, hostname, datacenter, rack))


def pool_in_instance_slowly(_run_cookbook, fqdn: str, hostname: str, phabricator_task_id: int) -> None:
    step("pool", f"Pooling in {fqdn}")
    reason = f"Pool {fqdn} in after cloning"
    task = f"T{phabricator_task_id}"
    _run_cookbook("sre.mysql.pool", ["--slow", "--reason", reason, "--task-id", task, hostname], confirm=True)


def _check_if_target_is_already_on_dbctl(dbctl: Dbctl, hostname: str, section: str) -> bool:
    """Return True when ready"""
    dbci = dbctl.instance.get(hostname)
    if dbci is None:
        msg = f"""Target host {hostname} is not known to dbctl.
Create a new entry for {hostname} in Puppet in
conftool-data/dbconfig-instance/instances.yaml then review and merge it.
For an example see:
https://gerrit.wikimedia.org/r/c/operations/puppet/+/663570/4/conftool-data/dbconfig-instance/instances.yaml

Also update the hieradata/hosts/{hostname}.yaml file in Puppet with the following text:
-----
# {hostname}
# {section}
mariadb::shard: '{section}'
-----
"""
        log.info(msg)
        return False

    if section not in dbci.sections:
        # TODO: support parsercache & others
        msg = f"""Target host {hostname} is known to dbctl but it has the following sections configured:
{dbci.sections}

Update the hieradata/hosts/{hostname}.yaml file in Puppet with the following text:
-----
# {hostname}
# {section}
mariadb::shard: '{section}'
-----
"""
        log.info(msg)
        return False

    if dbci.sections[section]["pooled"]:
        log.warn(f"{hostname} is already pooled in according to {dbci.sections}")

    return True


def check_pooling_status(
    dbctl: Dbctl,
    hostname: str,
    fqdn: str,
    expect_pooled=True,
    reject_candidate_master=True,
) -> str:
    """
    Check pooling status, extract section, reject unsuitable instances
    """
    dbci = dbctl.instance.get(hostname)
    ensure(dbci is not None, f"{hostname} not found in dbctl")
    sections = dbci.sections
    ensure(len(sections) == 1, f"{fqdn} has sections: '{sections.keys()}'")
    sec_name: str = list(sections.keys())[0]
    sd: Dict = sections[sec_name]
    if expect_pooled:
        if not sd["pooled"]:
            ask_confirmation(f"{fqdn} is not pooled")
    else:
        ensure(not sd["pooled"], f"{fqdn} is pooled")

    if reject_candidate_master:
        ensure(not sd.get("candidate_master"), f"{fqdn} is candidate master")

    return sec_name


def _wait_for_replication_lag_to_lower(log: Logger, instance: MInst) -> None:
    while True:
        try:
            replag = int(instance.replication_lag())
        except Exception:  # pylint: disable=W0718
            log.debug("Unable to extract replication lag", exc_info=True)
            time.sleep(10)
            continue

        if replag <= 1:
            log.info("Replication is healthy")
            return

        if (replag is None) or (replag > 1.0):
            log.debug(f"Replication lag: {replag}s - waiting 10s to catch up")
            time.sleep(10)


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
        parser.add_argument("--source", help="Cumin query to match the host of the source.", required=True)
        parser.add_argument("--target", help="Cumin query to match the host of the target.", required=True)
        parser.add_argument("--task", help="Phabricator task", required=True)
        parser.add_argument("--nopool", action="store_true", help="Do not pool in target host")
        h = "Do not check if target host already exists in zarcillo"
        parser.add_argument("--ignore-existing", action="store_true", help=h)
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return CloneMySQLRunner(args, self.spicerack)


def _fetch_netbox_data(spicerack: Spicerack, hn: str) -> Tuple[bool, str, str]:
    """Return netbox active flag, datacenter and rack name"""
    n = spicerack.netbox_server(hn)
    nd = n.as_dict()
    datacenter = nd["site"]["slug"]
    rack_name = nd["rack"]["name"]
    return (n.status == "active", datacenter, rack_name)


def _fetch_primary_fqdn(mysql: Mysql, fqdn: str) -> str:
    """Given a replica, find where it's replicating from"""
    inst = get_db_instance(mysql, fqdn)
    status = inst.show_slave_status()
    p = status.get("Master_Host", "")
    ensure(p, f"master host for {fqdn} not found in SHOW SLAVE STATUS")
    return p


def _wait_until_target_dbctl_conf_is_good(dbctl: Dbctl, hostname: str, section: str) -> None:
    for attempt in range(144):  # 24h
        if _check_if_target_is_already_on_dbctl(dbctl, hostname, section) is True:
            return
        log.info(f"[{attempt}] Polling again in 10 mins.")
        time.sleep(60 * 10)
    raise TimeoutError("Timed out")


class CloneMySQLRunner(CookbookRunnerBase):
    """Clone MySQL cookbook runner."""

    def __init__(self, args, spicerack: Spicerack):
        """Clone one MySQL host into another."""
        if sys.flags.optimize:
            print("Running python with -O is not supported")
            sys.exit(1)
        ensure_shell_is_durable()

        self._mysql = spicerack.mysql()
        self.alerting_hosts = spicerack.alerting_hosts
        self.admin_reason = spicerack.admin_reason("MySQL Clone")
        self.remote = spicerack.remote()
        self._phab = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self._run_cookbook = spicerack.run_cookbook

        self.source_hostname, self.source_fqdn = parse_db_host_fqdn(args.source)
        self.source_host = _fetch_db_remotehost(self.remote, self.source_fqdn)

        self.target_hostname, self.target_fqdn = parse_db_host_fqdn(args.target)
        self.target_host = _fetch_db_remotehost(self.remote, self.target_fqdn)

        primary_fqdn = _fetch_primary_fqdn(self._mysql, self.source_fqdn)
        self.primary_hostname, self.primary_fqdn = parse_db_host_fqdn(primary_fqdn)
        self.primary_host = _fetch_db_remotehost(self.remote, self.primary_fqdn)

        self.phabricator_task_id = parse_phabricator_task(args.task)

        self.pool_in_target = not args.nopool

        self.puppet = spicerack.puppet
        self.logger = logging.getLogger(__name__)

        # Other prep
        self.tp_options = dict(transferpy.transfer.parse_configurations(transferpy.transfer.CONFIG_FILE))
        # this also handles string->bool conversion where necessary
        self.tp_options = transferpy.transfer.assign_default_options(self.tp_options)

        (src_active, src_site, _) = _fetch_netbox_data(spicerack, self.source_hostname)
        (tgt_active, self.target_site, self.target_rack_name) = _fetch_netbox_data(spicerack, self.target_hostname)
        (_, prim_site, _) = _fetch_netbox_data(spicerack, self.primary_hostname)

        ensure(self.target_site == prim_site, f"Target site {self.target_site} does not match primary {prim_site}")

        # Ensure hosts are flagged as active on netbox
        ensure(src_active, f"{self.source_hostname} not active on netbox")
        ensure(tgt_active, f"{self.target_hostname} not active on netbox")

        # Ensure hosts are in the same dc otherwise encrypt
        if src_site != self.target_site:
            ask_confirmation("Source and target are in different datacenters! Continue?")
            self.tp_options["encrypt"] = True

        # Load replication credentials
        config = load_yaml_config(spicerack.config_dir / "mysql" / "config.yaml")
        self.replication_user = config["replication_user"]
        self.replication_password = config["replication_password"]

        self.admin_reason = spicerack.admin_reason(f"Cloning MariaDB T{self.phabricator_task_id}")
        self._dbctl: Dbctl = spicerack.dbctl()

        self._target_icinga_host = spicerack.icinga_hosts(self.target_host.hosts)

        step("check", "Running pre-flight checks")

        if not args.ignore_existing:
            ensure_db_not_in_zacillo(self._mysql, self.target_fqdn, self.target_hostname)

        # TODO: check if target is pooled in

        # Check DB roles in Zarcillo and cross-reference their sections
        source_section_z = check_db_role_on_zarcillo(self._mysql, self.source_fqdn)
        primary_section_z = check_db_role_on_zarcillo(self._mysql, self.primary_fqdn, expect_db_is_master=True)
        ensure(
            source_section_z == primary_section_z,
            "Primary and source DB are in different sections",
        )

        step("check", "Checking current pooling status")
        source_section = check_pooling_status(self._dbctl, self.source_hostname, self.source_fqdn)
        self.primary_section = check_pooling_status(self._dbctl, self.primary_hostname, self.primary_fqdn)
        ensure(source_section == source_section_z, "Inconsistent section")
        ensure(self.primary_section == primary_section_z, "Inconsistent section")

        if _check_if_target_is_already_on_dbctl(self._dbctl, self.target_hostname, source_section) is False:
            log.info("Note: you can update the section configured in puppet during the cloning")

        print(f"*** Preparing to clone {self.source_fqdn} to {self.target_fqdn} on section {source_section} ***")

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return f"of {self.source_host} onto {self.target_host}"

    def run(self) -> None:
        """Required by the Spicerack API."""
        # Guard against useless conftool messages
        logging.getLogger("conftool").setLevel(logging.WARNING)

        print("Open Grafana for the source: %s" % gen_grafana_mysql_url(self.source_hostname))
        print("Open Grafana for the target: %s" % gen_grafana_mysql_url(self.target_hostname))

        ask_confirmation(f"Ready to depool {self.source_fqdn}?")
        msg = f"Started cloning {self.source_fqdn} to {self.target_fqdn} - {self.admin_reason.owner}"
        self._phab.task_comment(str(self.phabricator_task_id), msg)

        step("depool", f"Depooling {self.source_fqdn}")
        reason = f"Depool {self.source_fqdn} to then clone it to {self.target_fqdn} - {self.admin_reason.owner}"
        task = f"T{self.phabricator_task_id}"
        self._run_cookbook(
            "sre.mysql.depool", ["--reason", reason, "--task-id", task, self.target_hostname], confirm=True
        )

        step("icinga", "Disabling monitoring for source and target host")
        alerters = self.alerting_hosts(self.source_host.hosts | self.target_host.hosts)
        downtime_id = alerters.downtime(self.admin_reason, duration=timedelta(hours=8))
        alerters.downtime(self.admin_reason, duration=timedelta(hours=48))

        step("clone", "Running the cloning tool")
        self._run_clone()

        step("zarc", f"Adding {self.target_fqdn} to Zarcillo")
        _add_host_to_zarcillo(
            self._mysql,
            self.target_hostname,
            self.target_fqdn,
            self.target_site,
            self.target_rack_name,
            self.primary_section,
        )

        step("catchup_repl_s", f"Catching up replication lag on {self.source_fqdn} before removing icinga downtime")
        _wait_for_replication_lag_to_lower(self.logger, get_db_instance(self._mysql, self.source_fqdn))

        step("catchup_repl_t", f"Catching up replication lag on {self.target_fqdn} before removing icinga downtime")
        _wait_for_replication_lag_to_lower(self.logger, get_db_instance(self._mysql, self.target_fqdn))

        step("wait_icinga_s", f"Waiting for icinga to go green for {self.source_fqdn}")
        self._target_icinga_host.wait_for_optimal()

        step("wait_icinga_t", f"Waiting for icinga to go green for {self.target_fqdn}")
        self._target_icinga_host.wait_for_optimal()

        step("icinga", "Removing icinga 'downtime'")
        alerters.remove_downtime(downtime_id)

        _wait_until_target_dbctl_conf_is_good(self._dbctl, self.target_hostname, self.primary_section)

        ask_confirmation("Is the change to instances.yaml merged?")

        # TODO: wait until the host shows up on prometheus

        ask_confirmation("Ready to pool in the nodes. Monitor the Grafana charts.")
        pool_in_instance_slowly(self._run_cookbook, self.source_fqdn, self.source_hostname, self.phabricator_task_id)

        if self.pool_in_target:
            step("wait", "Waiting 1h before pooling in {target_fqdn}")
            time.sleep(3600)
            pool_in_instance_slowly(
                self._run_cookbook, self.target_fqdn, self.target_hostname, self.phabricator_task_id
            )

        msg = f"Finished cloning {self.source_fqdn} to {self.target_fqdn} - {self.admin_reason.owner}"
        self._phab.task_comment(str(self.phabricator_task_id), msg)
        step("done", "Done")

    def _run_clone(self) -> None:
        """
        Run the cloning process. Most of the heavy lifting is done by transfer.py
        """
        self.logger.info("Running STOP SLAVE on %s", self.source_hostname)
        self._run_scripts(self.source_host, ['mysql -e "STOP SLAVE;"'])
        # Sleep for a second to make sure the position is updated
        time.sleep(1)

        binlog_file, repl_position = _fetch_replication_status(self.source_host)

        self.logger.info("Stopping mariadb on %s", self.source_hostname)
        _run(self.source_host, "service mariadb stop")

        self.logger.info("Running STOP SLAVE on %s", self.target_hostname)
        _run(self.target_host, 'mysql -e "STOP SLAVE;"')

        self.logger.info("Stopping mariadb on %s", self.target_hostname)
        _run(self.target_host, "service mariadb stop")

        self.logger.info("Removing /srv/sqldata on %s", self.target_hostname)
        _run(self.target_host, "rm -rf /srv/sqldata/")

        self.logger.info("Starting transfer")
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

        _wait_for_replication_lag_to_lower(self.logger, get_db_instance(self._mysql, self.target_fqdn))

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

    def _run_scripts(self, host: RemoteHosts, scripts: List[str]) -> None:
        for script in scripts:
            try:
                _run(host, script)
            except AbortError:
                self.logger.error("%s: execution aborted", script)
                raise
