"""Perform all the Database related finalization steps after the switch datacenter."""
import logging

from spicerack.mysql_legacy import Instance, MasterUseGTID, MysqlLegacyError
from wmflib.actions import Actions
from wmflib.interactive import confirm_on_failure

from cookbooks.sre.switchdc.databases import DatabaseCookbookBase, DatabaseRunnerBase


logger = logging.getLogger(__name__)


class FinalizeRunner(DatabaseRunnerBase):
    """As required by Spicerack API."""

    def run_on_section(self, section: str, master_from: Instance, master_to: Instance):
        """Run all the steps on a given section."""
        FinalizeSection(
            section=section,
            dc_from=self.dc_from,
            master_from=master_from,
            master_to=master_to,
            actions=self.actions[section],
            dry_run=self.dry_run,
        ).run()


class FinalizeSection:
    """Perform all the finalization steps on a single section."""

    def __init__(  # pylint: disable=too-many-arguments
        self,
        *,
        section: str,
        dc_from: str,
        master_from: Instance,
        master_to: Instance,
        actions: Actions,
        dry_run: bool
    ):
        """Initialize the section runner."""
        self.section = section
        self.dc_from = dc_from
        self.master_from = master_from
        self.master_to = master_to
        self.actions = actions
        self.dry_run = dry_run

    def run(self):
        """Execute all the steps on a single instance."""
        confirm_on_failure(self.validate)
        confirm_on_failure(self.reset_replication)
        confirm_on_failure(self.clean_heartbeat)
        confirm_on_failure(self.enable_gtid)

    def validate(self) -> None:
        """Validate that the given hosts are indeed the two masters for that section."""
        from_host = str(self.master_from.host)
        to_host = str(self.master_to.host)

        read_only_from = self.master_from.run_vertical_query("SELECT @@read_only", is_safe=True)[0]["@@read_only"]
        logger.info("[%s] MASTER_FROM %s @@read_only=%s", self.section, from_host, read_only_from)
        if read_only_from != "1":
            message = f"MASTER_FROM {from_host} should be read only"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        read_only_to = self.master_to.run_vertical_query("SELECT @@read_only", is_safe=True)[0]["@@read_only"]
        logger.info("[%s] MASTER_TO %s @@read_only=%s", self.section, to_host, read_only_to)
        if read_only_to != "0":
            message = f"MASTER_TO {to_host} should be read write"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        if self.dry_run:
            logger.info("[%s] Skipping validation of MASTER_TO's master in DRY-RUN", self.section)
        else:
            master_to_status = self.master_to.show_slave_status()
            master_to_master = master_to_status["Master_Host"]
            logger.info("[%s] MASTER_TO %s master is %s", self.section, to_host, master_to_master)
            if master_to_master != from_host:
                message = f"MASTER_TO {to_host}'s master is not MASTER_FROM {from_host}, got {master_to_master} instead"
                self.actions.failure(f"**{message}**")
                raise RuntimeError(message)

        master_from_status = self.master_from.show_slave_status()
        master_from_master = master_from_status["Master_Host"]
        logger.info("[%s] MASTER_FROM %s master is %s", self.section, to_host, master_from_master)
        if master_from_master != to_host:
            message = f"MASTER_FROM {from_host}'s master is not MASTER_TO {to_host}, got {master_from_master} instead"
            self.actions.failure(f"**{message}**")
            raise RuntimeError(message)

        self.actions.success(f"Validated replication topology for section {self.section} between MASTER_TO "
                             f"{to_host} and MASTER_FROM {from_host}")

    def reset_replication(self):
        """Reset the replication on MASTER_TO."""
        try:
            status = self.master_to.show_slave_status()
        except MysqlLegacyError:  # Raised if the host is not replicating already
            self.actions.success(f"MASTER_TO {self.master_to.host} has no replication set, skipping.")
            return

        self.master_to.stop_slave()
        self.actions.success(f"MASTER_TO {self.master_to.host} STOP SLAVE.")
        self.master_to.run_query("RESET SLAVE ALL")
        self.actions.success(f"MASTER_TO {self.master_to.host} RESET SLAVE ALL.")

        try:
            status = self.master_to.show_slave_status()
            message = f"MASTER_TO {self.master_to.host} should not be replicating"
            logger.error("%s, got :\n%s", message, status)
            if self.dry_run:
                self.actions.success("Ignoring failed check for reset replication in DRY-RUN mode.")
            else:
                raise RuntimeError(f"[{self.section}] {message}")
        except MysqlLegacyError:  # Raised if the host is not replicating, as expected
            self.actions.success(f"MASTER_TO {self.master_to.host} has no replication set.")

    def clean_heartbeat(self):
        """Clean the MASTER_FROM rows in pt-heartbeat on MASTER_TO."""
        query = (f"SELECT server_id FROM heartbeat WHERE shard = '{self.section}' AND "
                 f"datacenter = '{self.dc_from}'")  # nosec
        rows = self.master_to.run_vertical_query(query, "heartbeat", is_safe=True)
        server_ids = [int(row["server_id"]) for row in rows]
        self.actions.success(f"MASTER_TO {self.master_from.host} heartbeat server IDs to delete are: {server_ids}")

        for server_id in server_ids:
            query = f"DELETE FROM heartbeat WHERE server_id={server_id}"  # nosec
            self.master_to.run_query(query, "heartbeat")
            self.actions.success(f"MASTER_TO {self.master_from.host} DELETED heartbeat rows for server ID {server_id}")

    def enable_gtid(self) -> None:
        """Enable GTID on the MASTER_FROM."""
        status = self.master_from.show_slave_status()
        if status["Using_Gtid"].lower() == MasterUseGTID.SLAVE_POS.value:
            self.actions.success(f"MASTER_FROM {self.master_from.host} has already GTID enabled, skipping.")
            return

        logger.info("[%s] Enabling GTID on MASTER_FROM %s", self.section, self.master_from.host)
        self.master_from.stop_slave()
        self.actions.success(f"MASTER_FROM {self.master_from.host} STOP SLAVE.")
        self.master_from.set_master_use_gtid(MasterUseGTID.SLAVE_POS)
        self.actions.success(f"MASTER_FROM {self.master_from.host} MASTER_USE_GTID={MasterUseGTID.SLAVE_POS.value}.")
        self.master_from.start_slave()
        self.actions.success(f"MASTER_FROM {self.master_from.host} START SLAVE.")

        status = self.master_from.show_slave_status()
        gtid = status["Using_Gtid"]
        logger.info("[%s] MASTER_FROM has now Using_Gtid: %s", self.section, gtid)
        if gtid.lower() != MasterUseGTID.SLAVE_POS.value:
            if self.dry_run:
                self.actions.success("Ignoring failed check for GTID change in DRY-RUN mode.")
            else:
                message = f"Failed to enable GTID on {self.master_from.host}, current value: {gtid}"
                self.actions.failure(f"**{message}**")
                raise RuntimeError(message)

        self.actions.success(f"Enabled GTID on MASTER_FROM {self.master_from.host}")


class Finalize(DatabaseCookbookBase):
    """Perform all the Database related finalization steps after the switch datacenter.

    Actions performed for each core sections (sX, x1, RW esX):
        * Enable replication on the master of the primary datacenter replicating from the master of the secondary
          datacenter.
        * Enable GTID on the master of the old primary datacenter, now secondary.

    ATTENTION: the arguments must be the same as the prepare step. This is still part of the migration from
    DC_FROM to DC_TO.

    Usage:
        cookbook sre.switchdc.databases.finalize -t T12345 eqiad codfw

    """

    runner_class = FinalizeRunner
