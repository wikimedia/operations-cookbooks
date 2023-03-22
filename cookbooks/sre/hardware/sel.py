"""cookbook to query and clear SEL events."""

import shlex

from datetime import date
from logging import getLogger

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError, RemoteError
from wmflib.interactive import ask_confirmation, AbortError

SENSOR_TYPES = (
    "Temperature",
    "Voltage",
    "Current",
    "Fan",
    "Physical_Security",
    "Platform_Security_Violation_Attempt",
    "Processor",
    "Power_Supply",
    "Power_Unit",
    "Cooling_Device",
    "Other_Units_Based_Sensor",
    "Memory",
    "Drive_Slot",
    "POST_Memory_Resize",
    "System_Firmware_Progress",
    "Event_Logging_Disabled",
    "Watchdog_1",
    "System_Event",
    "Critical_Interrupt",
    "Button_Switch",
    "Module_Board",
    "Microcontroller_Coprocessor",
    "Add_In_Card",
    "Chassis",
    "Chip_Set",
    "Other_Fru",
    "Cable_Interconnect",
    "Terminator",
    "System_Boot_Initiated",
    "Boot_Error",
    "OS_Boot",
    "OS_Critical_Stop",
    "Slot_Connector",
    "System_ACPI_Power_State",
    "Watchdog_2",
    "Platform_Alert",
    "Entity_Presence",
    "Monitor_ASIC_IC",
    "LAN",
    "Management_Subsystem_Health",
    "Battery",
    "Session_Audit",
    "Version_Change",
    "FRU_State",
    "OEM_Reserved",
)


class Sel(CookbookBase):
    """Interact with host SEL.

    Usage:
        cookbook sre.hardware.sel
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()

        subparsers = parser.add_subparsers(dest="action", required=True)
        query = subparsers.add_parser(
            "query", help="Query SEL for specific information"
        )
        query.add_argument(
            "-F",
            "--from-date",
            type=date.fromisoformat,
            help="Find entries starting from this date (YYYY-MM-DD).",
            default=date.fromtimestamp(0),
        )
        query.add_argument(
            "-T",
            "--to-date",
            type=date.fromisoformat,
            help="Find entries before from this date (YYYY-MM-DD)",
            default=date.today(),
        )
        query.add_argument(
            "-i",
            "--include-type",
            choices=SENSOR_TYPES,
            nargs="+",
            help="Only include entries with this type. Can pass multiple times",
        )
        query.add_argument(
            "-e",
            "--exclude-type",
            choices=SENSOR_TYPES,
            nargs="+",
            help="Exclude entries with this type. Can pass multiple times",
        )
        query.add_argument(
            "--clear",
            action="store_true",
            help="Clear all matching events",
        )

        query.add_argument("query", help="A cumin query of hosts to audit.")
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return SelRunner(args, self.spicerack)


class SelRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the SEL runner."""
        if args.clear:
            if args.from_date.date != args.from_date:
                ask_confirmation(
                    f"Clear can only work on full days. Converting --from-date to {args.from_date.date}"
                )
                args.from_date = args.from_date.date
            if args.to_date.date != args.to_date:
                ask_confirmation(
                    f"Clear can only work on full days. Converting --to-date to {args.to_date.date}"
                )
                args.from_date = args.from_date.date

        self.command = [
            "ipmi-sel",
            "--comma-separated-output",
            "--non-abbreviated-units",
            "--no-header-output",
            "--interpret-oem-data",
            "--entity-sensor-names",
            f"--date-range={args.from_date.strftime('%m/%d/%Y')}-{args.to_date.strftime('%m/%d/%Y')}",
        ]
        if args.include_type:
            self.command.append(f"--sensor-types={','.join(args.include_type)}")
        if args.exclude_type:
            self.command.append(f"--exclude-sensor-types={','.join(args.exclude_type)}")
        self.args = args
        self.logger = getLogger(__name__)
        try:
            self.hosts = spicerack.remote().query(args.query)
        except RemoteError as error:
            raise RuntimeError("No hosts found matching {args.query}") from error

    def run(self):
        """Main run method either query or clear SEL events."""
        try:
            results = self.hosts.run_sync(shlex.join(self.command), print_output=False, is_safe=True)
        except RemoteExecutionError as e:
            results = e.results
        found_events = False
        for host, result in results:
            lines = result.message().decode().splitlines()
            if not lines:
                continue
            found_events = True
            for line in lines:
                try:
                    event_id, event_date, time, name, event_type, event = line.split(',')
                except ValueError:
                    # likley a failed cumin run
                    self.logger.warning('%s: failed to get output: %s', host, line)
                    break
                print(f"{host}:")
                print(f"{event_id}| {event_date} {time}|{name}| {name} ({event_type})| {event}")
        if found_events and self.args.clear:
            try:
                ask_confirmation("Are you sure you would like to clear all the above events")
                self.command.append("--clear")
                self.hosts.run_sync(shlex.join(self.command), print_output=False)
            except AbortError:
                self.logger.warning("clear aborted")
