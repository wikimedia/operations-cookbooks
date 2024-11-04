"""Perform all the Database related preparatory steps before the switch datacenter."""
import logging
from datetime import timedelta
from time import sleep

from pymysql.err import MySQLError

from spicerack.mysql_legacy import Instance, MasterUseGTID, ReplicationInfo
from wmflib.actions import Actions
from wmflib.config import load_yaml_config
from wmflib.interactive import confirm_on_failure

from cookbooks.sre.switchdc.databases import DatabaseCookbookBase, DatabaseRunnerBase


logger = logging.getLogger(__name__)


class PrepareRunner(DatabaseRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        super().__init__(args, spicerack)
        self.puppet = spicerack.puppet
        self.alerting_hosts = spicerack.alerting_hosts
        config = load_yaml_config(spicerack.config_dir / "cookbooks" / "sre.switchdc.databases.yaml")
        self.user = config["repl_user"]
        self.password = config["repl_pass"]

    def run_on_section(self, section: str, master_from: Instance, master_to: Instance):
        """Run all the steps on a given section."""
        downtime_hosts = self.remote.query(f"A:db-section-{section}").hosts
        with self.alerting_hosts(downtime_hosts).downtimed(self.reason, duration=timedelta(minutes=30)):
            with self.puppet(master_to.host).disabled(self.reason):
                PrepareSection(
                    section=section,
                    master_from=master_from,
                    master_to=master_to,
                    user=self.user,
                    password=self.password,
                    actions=self.actions[section],
                    dry_run=self.dry_run,
                ).run()


class PrepareSection:
    """Perform all the preparatory steps on a single section."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        section: str,
        master_from: Instance,
        master_to: Instance,
        user: str,
        password: str,
        actions: Actions,
        dry_run: bool
    ):
        """Initialize the section runner."""
        self.section = section
        self.master_from = master_from
        self.master_to = master_to
        self.user = user
        self.password = password
        self.actions = actions
        self.dry_run = dry_run

    def run(self) -> None:
        """Execute all the steps on a single instance."""
        confirm_on_failure(self.validate)
        confirm_on_failure(self.disable_gtid)
        confirm_on_failure(self.master_to_stop_replication)
        master_to_position = confirm_on_failure(self.wait_master_to_position)
        confirm_on_failure(self.enable_circular_replication, master_to_position)
        confirm_on_failure(self.master_to_restart_replication)
        confirm_on_failure(self.master_from_check_replication)

    def validate(self) -> None:
        """Validate that the given hosts are indeed the two masters for that section."""
        from_host = str(self.master_from.host)
        to_host = str(self.master_to.host)

        read_only_from = self.master_from.fetch_one_row("SELECT @@read_only")["@@read_only"]
        logger.info("[%s] MASTER_FROM %s @@read_only=%s", self.section, from_host, read_only_from)
        if read_only_from != 0:
            message = f"MASTER_FROM {from_host} should be read write"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        read_only_to = self.master_to.fetch_one_row("SELECT @@read_only")["@@read_only"]
        logger.info("[%s] MASTER_TO %s @@read_only=%s", self.section, to_host, read_only_to)
        if read_only_to != 1:
            message = f"MASTER_TO {to_host} should be read only"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        master_to_status = self.master_to.show_slave_status()
        master_to_master = master_to_status["Master_Host"]
        logger.info("[%s] MASTER_TO %s master is %s", self.section, to_host, master_to_master)
        if master_to_master != from_host:
            message = f"MASTER_TO {to_host}'s master is not MASTER_FROM {from_host}, got {master_to_master} instead"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        with self.master_to.cursor() as (_connection, cursor):
            _ = cursor.execute("SHOW SLAVE HOSTS")
            master_to_replicas = cursor.fetchall()
            self.master_to.check_warnings(cursor)

        for replica in master_to_replicas:
            logger.info("[%s] Checking MASTER_TO %s replica %s", self.section, to_host, replica)
            if replica["Host"].split(".")[1] != to_host.split(".")[1]:
                message = (f"Expected all replicas of MASTER_TO {to_host} to be in the same datacenter, "
                           f"got {replica['Host']} instead")
                self.actions.failure(f"**{message}**")
                raise RuntimeError(message)

        logger.info("[%s] Checking binlog format is the same in both MASTER_TO %s and MASTER_FROM %s",
                    self.section, from_host, to_host)
        binlog_query = "SELECT @@GLOBAL.binlog_format AS binlog_format"
        try:
            binlog_from = self.master_from.fetch_one_row(binlog_query)["binlog_format"]
        except MySQLError:
            binlog_from = "UNDEFINED"

        try:
            binlog_to = self.master_to.fetch_one_row(binlog_query)["binlog_format"]
        except MySQLError:
            binlog_to = "UNDEFINED"

        if binlog_from != binlog_to:
            message = (f"Binlog format mistmatch between MASTER_FROM {from_host} {binlog_from} "
                       f"and MASTER_TO {to_host} {binlog_to}.")
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        self.actions.success(f"Validated replication topology for section {self.section} between MASTER_FROM "
                             f"{from_host} and MASTER_TO {to_host}")

    def disable_gtid(self) -> None:
        """Disable GTID on the MASTER_TO."""
        logger.info("[%s] Disabling GTID on MASTER_TO %s", self.section, self.master_to.host)
        self.master_to.stop_slave()
        self.actions.success(f"MASTER_TO {self.master_to.host} STOP SLAVE.")
        self.master_to.set_master_use_gtid(MasterUseGTID.NO)
        self.actions.success(f"MASTER_TO {self.master_to.host} MASTER_USE_GTID={MasterUseGTID.NO.value}.")
        self.master_to.start_slave()
        self.actions.success(f"MASTER_TO {self.master_to.host} START SLAVE.")

        expected = {
            "Slave_IO_Running": "Yes",
            "Slave_SQL_Running": "Yes",
            "Last_IO_Errno": 0,
            "Last_SQL_Errno": 0,
            "Using_Gtid": MasterUseGTID.NO.value.capitalize(),
        }
        status = self.master_to.show_slave_status()
        try:
            self._validate_slave_status(f"MASTER_TO {self.master_to.host}", status, expected)
        except RuntimeError as e:
            message = f"Failed to verify disabled GTID on {self.master_to.host}"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message) from e

        self.actions.success(f"Disabled GTID on MASTER_TO {self.master_to.host}")

    def master_to_stop_replication(self) -> None:
        """Stop pt-heartbeat and the replication on MASTER_TO to prepare it to enable replication from MASTER_FROM."""
        self.master_to.host.run_sync("/bin/systemctl stop pt-heartbeat-wikimedia.service")
        self.actions.success(f"MASTER_TO {self.master_to.host} stopped pt-heartbeat.")
        self.master_to.stop_slave()
        self.actions.success(f"MASTER_TO {self.master_to.host} STOP SLAVE.")

    def wait_master_to_position(self) -> dict:
        """Waits until the MASTER_TO master position is stable and return it."""
        logger.info(
            "[%s] Checking if MASTER_TO %s master position is stable over time", self.section, self.master_to.host)
        stable = 0
        current = self.master_to.show_master_status()
        logger.info("[%s] MASTER_TO %s master position is: %s", self.section, self.master_to.host, current)
        attempts = 2 if self.dry_run else 11
        for i in range(1, attempts):
            logger.info("Sleeping 3 seconds")
            sleep(3)
            new = self.master_to.show_master_status()
            if new == current:
                stable += 1
            else:
                stable = 0  # Reset the counter

            current = new
            logger.info("[%s] (%d/%d) MASTER_TO %s master position is: %s",
                        self.section, i, attempts - 1, self.master_to.host, current)

            if stable >= 2:  # It means we got 3 consecutive readings that are the same with 3s sleep in between them
                self.actions.success(f"MASTER_TO {self.master_to.host} MASTER STATUS is stable over time: {current}")
                return current

        if self.dry_run:
            self.actions.success(f"MASTER_TO {self.master_to.host} Ignoring MASTER STATUS is not stable in DRY-RUN")
            return current

        message = f"MASTER_TO {self.master_to.host} MASTER STATUS is not stable, see the extended logs"
        self.actions.failure(f"**{message}**")
        raise RuntimeError(message)

    def enable_circular_replication(self, master_to_position: dict) -> None:
        """Changes the master on MASTER_FROM to replicate from MASTER_TO and enable circular replication."""
        repl_info = ReplicationInfo(
            primary=str(self.master_to.host),
            binlog=master_to_position["File"],
            position=int(master_to_position["Position"]),
            port=3306,  # TBD: do we need to make it dynamic reading @@PORT?
        )
        logger.info("[%s] MASTER_FROM %s CHANGE MASTER to %s", self.section, self.master_from.host, repl_info)
        message = f"MASTER_FROM {self.master_from.host} CHANGE MASTER to {repl_info} and user {self.user}"
        # TODO: TEMPORARY HACK START to prevent leaking the password in the logs TO BE MOVED INTO SPICERACK
        cumin_logger = logging.getLogger("cumin")
        remote_logger = logging.getLogger("spicerack.remote")
        cumin_logger_level = cumin_logger.getEffectiveLevel()
        remote_logger_level = remote_logger.getEffectiveLevel()
        cumin_logger.info(message)
        cumin_logger.info("Temporarily setting cumin logging to ERROR level to prevent password leaking")
        cumin_logger.setLevel(logging.ERROR)
        remote_logger.setLevel(logging.ERROR)
        # =====
        self.master_from.set_replication_parameters(replication_info=repl_info, user=self.user, password=self.password)
        # =====
        cumin_logger.setLevel(cumin_logger_level)
        remote_logger.setLevel(remote_logger_level)
        # TODO: TEMPORARY HACK END
        self.actions.success(f"MASTER_FROM {self.master_from.host} CHANGE MASTER to {repl_info} and user {self.user}")

        self.master_from.start_slave()
        self.actions.success(f"MASTER_FROM {self.master_from.host} START SLAVE")

        if self.dry_run:  # SHOW SLAVE STATUS would fail without replication set
            self.actions.success(
                f"MASTER_FROM {self.master_from.host} skipping replication from MASTER_TO "
                f"{self.master_to.host} verification"
            )
            return

        logger.info("[%s] MASTER_FROM %s verifying replication is running", self.section, self.master_from.host)
        expected = {
            "Master_Host": str(self.master_to.host),
            "Master_User": self.user,
            "Master_Port": 3306,
            "Master_Log_File": master_to_position["File"],
            "Read_Master_Log_Pos": master_to_position["Position"],
            "Exec_Master_Log_Pos": master_to_position["Position"],
            "Slave_IO_Running": "Yes",
            "Slave_SQL_Running": "Yes",
            "Last_IO_Errno": 0,
            "Last_SQL_Errno": 0,
        }
        status = self.master_from.show_slave_status()
        self._validate_slave_status(f"MASTER_FROM {self.master_from.host}", status, expected)
        self.actions.success(
            f"MASTER_FROM {self.master_from.host} replication from MASTER_TO {self.master_to.host} verified")

    def master_to_restart_replication(self) -> None:
        """Restart the replication on MASTER_TO and its pt-heartbeat."""
        self.master_to.host.run_sync("/bin/systemctl start pt-heartbeat-wikimedia.service")
        self.actions.success(f"MASTER_TO {self.master_to.host} started pt-heartbeat.")
        self.master_to.start_slave()
        self.actions.success(f"MASTER_TO {self.master_to.host} START SLAVE.")
        expected = {
            "Master_Host": str(self.master_from.host),
            "Master_User": self.user,
            "Master_Port": 3306,
            "Slave_IO_Running": "Yes",
            "Slave_SQL_Running": "Yes",
            "Last_IO_Errno": 0,
            "Last_SQL_Errno": 0,
        }
        status = self.master_to.show_slave_status()
        self._validate_slave_status(f"MASTER_TO {self.master_to.host}", status, expected)
        self.actions.success(
            f"MASTER_TO {self.master_to.host} replication from MASTER_FROM {self.master_from.host} verified")

    def master_from_check_replication(self) -> None:
        """Check the replication on MASTER_FROM."""
        if self.dry_run:  # SHOW SLAVE STATUS would fail without replication set
            self.actions.success(
                f"MASTER_FROM {self.master_from.host} skipping replication from MASTER_TO "
                f"{self.master_to.host} verification after pt-heartbeat"
            )
            return

        expected = {
            "Master_Host": str(self.master_to.host),
            "Master_User": self.user,
            "Master_Port": 3306,
            "Slave_IO_Running": "Yes",
            "Slave_SQL_Running": "Yes",
            "Last_IO_Errno": 0,
            "Last_SQL_Errno": 0,
        }
        status = self.master_from.show_slave_status()
        self._validate_slave_status(f"MASTER_FROM {self.master_from.host}", status, expected)
        self.actions.success(
            f"MASTER_FROM {self.master_from.host} replication from MASTER_TO {self.master_to.host} verified after "
            "pt-heartbeat"
        )

    def _validate_slave_status(self, prefix: str, status: dict, expected: dict):
        """Ensure that SHOW SLAVE STATUS provided keys have the expected values."""
        for key, value in expected.items():
            logger.info("[%s] %s checking SLAVE STATUS %s=%s", self.section, prefix, key, status[key])
            if status[key] != value:
                if self.dry_run:
                    self.actions.success(f"{prefix} Ignoring wrong SLAVE STATUS in DRY-RUN mode {key}={status[key]}, "
                                         f"expected {value} instead.")
                else:
                    message = f"{prefix} wrong SLAVE STATUS {key}={status[key]}, expected {value} instead"
                    self.actions.failure(f"**{message}**")
                    raise RuntimeError(message)


class Prepare(DatabaseCookbookBase):
    """Perform all the Database related preparatory steps before the switch datacenter.

    By default actions are performed for each core sections (sX, x1, RW esX):
        * Downtime the whole database section.
        * Disable puppet on the master of the secondary datacanter.
        * Disable GTID on the master of the secondary datacenter.
        * Stop replication on the master of the secondary datacenter.
        * Stop pt-heartbeat on the master of the secondary datacenter.
        * Enable replication on the master of the primary datacenter replicating from the master of the secondary
          datacenter.
        * Start pt-heartbeat on the master of the secondary datacenter.
        * Start replication on the master of the secondary datacenter.
        * Re-enable puppet on the master of the secondary datacenter.

    Usage:
        cookbook sre.switchdc.databases.prepare -t T12345 eqiad codfw

    """

    runner_class = PrepareRunner
