"""Divert all traffic from a datacenter."""
import argparse
import logging
import time

from dataclasses import dataclass

from spicerack import Spicerack
from spicerack.administrative import Reason
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.dnsdisc import DiscoveryCheckError, DiscoveryError
from spicerack.remote import RemoteHosts
from spicerack.service import ServiceDiscoveryRecord, ServiceIPs
from spicerack.confctl import ConfctlError
from wmflib.constants import CORE_DATACENTERS
from wmflib.interactive import ask_input, ask_confirmation, confirm_on_failure, ensure_shell_is_durable, InputError

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.discovery import resolve_with_client_ip, DC_IP_MAP
from cookbooks.sre.switchdc.mediawiki import MEDIAWIKI_SERVICES


logger = logging.getLogger(__name__)
# This is used in DiscoveryDcRouteRunner._get_all_services, but might be of use for
# other cookbooks too, so make it a module constant.
EXCLUDED_SERVICES = {
    "blubberoid": "blubberoid needs to follow swift replica for the docker registry",
    "docker-registry": "swift replica goes codfw => eqiad and needs manual switching",
    "releases": "not a 'service', strictly speaking, thus excluded",
    "puppetdb-api": "not a 'service', strictly speaking, thus excluded",
    "helm-charts": "not a 'service', strictly speaking, thus excluded",
    "toolhub": "T288685: needs to match m5 database cluster replication",
    "wdqs": "T329193: capacity limitations in codfw",
    "wdqs-ssl": "T329193: capacity limitations in codfw",
}


@dataclass(frozen=True)
class DiscoveryRecord:
    """Encapsulates the two objects from servicecatalog we need."""

    record: ServiceDiscoveryRecord
    ips: ServiceIPs

    def depool(self, datacenter: str):
        """Depools the record from the given datacenter."""
        self.record.instance.depool(datacenter)

    def pool(self, datacenter: str):
        """Pools the record in the given datacenter."""
        self.record.instance.pool(datacenter)

    def clear_cache(self, recursors: RemoteHosts):
        """Clears the DNS resolver caches for this record."""
        recursors.run_sync(f"sudo rec_control wipe-cache {self.fqdn}")

    def clean_discovery_templates(self, authdns: RemoteHosts):
        """Removes spurious dns discovery errors when switching A/P services."""
        if self.active_active:
            logger.debug("NOT clearing confd templates for %s as it's an active/active service.", self.name)
            return
        authdns.run_sync(f"rm -fv /var/run/confd-template/.discovery-{self.name}.state*.err")

    @retry(backoff_mode="constant", exceptions=(DiscoveryCheckError, DiscoveryError), tries=15)
    def check_records(self):
        """Check the DNS records.

        For every datacenter the service is present in, we check that:
        * If the datacenter is pooled, resolving the name from a client in that datacenter
          returns the local IP of the service
        * If it's depooled, resolving the name from a client in that datacenter returns a
          non-local ip for the service.

        The most important function of this check is to ensure the etcd change has been
        propagated before we cleare the dns recursor caches.

        Raises: DiscoveryCheckError on failure
        """
        # for each service that is depooled, check that the ip returned by the
        # authoritative resolver is not the one in the same DC.
        # Opposite for pooled ones.
        current_state = self.state
        for datacenter in self.ips.sites:
            expected_ip = str(self.ips.get(datacenter))

            for dns_answer in resolve_with_client_ip(self.record.instance, DC_IP_MAP[datacenter], self.name):
                actual_ip = dns_answer[0].address
                if datacenter in current_state and actual_ip != expected_ip:
                    raise DiscoveryCheckError(
                        f"Error checking auth dns for {self.fqdn} in {datacenter}: "
                        f"resolved to {actual_ip}, expected: {expected_ip}"
                    )
                if datacenter not in current_state and actual_ip == expected_ip:
                    raise DiscoveryCheckError(
                        f"Error checking auth dns for {self.fqdn} in {datacenter}: "
                        f"resolved to {expected_ip}, a different IP was expected."
                    )

    @property
    def fqdn(self) -> str:
        """The fqdn of the record"""
        return f"{self.name}.discovery.wmnet"

    @property
    def active_active(self) -> bool:
        """AA or AP"""
        return self.record.active_active

    @property
    def type(self) -> str:
        """String representation of service type"""
        return "Active/Active" if self.active_active else "Active/Passive"

    @property
    def name(self) -> str:
        """The dnsdisc name"""
        return self.record.dnsdisc

    @property
    def state(self) -> set[str]:
        """The state of the dnsdisc object"""
        return set(self.record.instance.active_datacenters[self.name])

    def is_pooled_in_dc(self, datacenter) -> bool:
        """True if dnsdisc object is pooled in datacenter"""
        return datacenter in self.state

    def __str__(self) -> str:
        """String representation"""
        return f"{self.name} ({self.type})"


class DiscoveryDcRoute(CookbookBase):
    """Pool/Depool a datacenter from internal traffic.

    This cookbook automates DNS Discovery operations like pool and depool of
    an entire core datacenter.

    Examples:
    - Depool all a/a discovery records in codfw:
      cookbook.sre.discovery.datacenter depool codfw

    - Pool all a/a discovery records in eqiad
      cookbook.sre.discovery.datacenter pool eqiad [--reason REASON] [--task-id T12345]

    - Depool ALL discovery records from codfw:
      cookbook.sre.discovery.datacenter depool codfw --all

    - Check which services are pooled in a datacenter:
      cookbook.sre.discovery.datacenter status codfw

    - Check which services are pooled in all datacenters:
      cookbook.sre.discovery.datacenter status all

    *In case of emergency only*: use the `--fast-insecure` switch for pool/depool,
    it will be making the cookbook much faster.

    When called without the --all switch, this cookbook will change the pooled
    state of all active-active to the desired state. It will prevent you from depooling
    a service that's only active in this datacenter, asking you if you prefer to skip
    it or move it to another datacenter. After every migration it will wipe the resolver caches to make
    the move more aggressive as this cookbook can be used in emergency situations.

    When called with the --all switch, this cookbook will try to also move all active/passive services
    with the notable exception of mediawiki-related services.

    When migrating an active-passive service, the cookbook will first set both datacenters to pooled, which
    will not trigger a dns change, then depool the desired datacenter, and only then wipe the resolver caches.

    """

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = super().argument_parser()
        actions = parser.add_subparsers(dest="action", help="The action to perform")

        for act in ["pool", "depool"]:
            action = actions.add_parser(act)
            action.add_argument(
                "datacenter", choices=CORE_DATACENTERS, help="Name of the datacenter. One of: %(choices)s."
            )
            action.add_argument(
                "--all", action="store_true", help="Depool also the active/passive services (minus MediaWiki)"
            )
            action.add_argument("--fast-insecure", "-f", help="Run the commands faster but relatively insecurely.")
            action.add_argument("-r", "--reason", required=False, help="Admin reason", default="maintenance")
            action.add_argument("-t", "--task-id", help="the Phabricator task ID to update and refer (i.e.: T12345)")
        status = actions.add_parser("status")
        status.add_argument(
            "datacenter", choices=CORE_DATACENTERS + ("all",), help="Name of the datacenter. One of: %(choices)s."
        )
        status.add_argument("--filter", action="store_true", help="Filter the excluded services.")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        if not self.spicerack.verbose:
            # Avoid conftool logs to flood INFO/DEBUG
            logging.getLogger("conftool").setLevel(logging.WARNING)
        if args.action == "status":
            args.all = True
            args.fast_insecure = False
            args.task_id = None
            args.reason = None
            if not self.spicerack.verbose:
                # Cleaner output when running status in dry-run.
                logger.setLevel(logging.WARNING)
                logging.getLogger("etcd").setLevel(logging.WARNING)
                logging.getLogger("spicerack.confctl").setLevel(logging.WARNING)
        else:
            args.filter = True
            ensure_shell_is_durable()
        return DiscoveryDcRouteRunner(args, self.spicerack)


class DiscoveryDcRouteRunner(CookbookRunnerBase):
    """Pool/Depool/Check services via DNS Discovery operations runner class."""

    def __init__(self, args: argparse.Namespace, spicerack: Spicerack):
        """Set up a runner for the desired action."""
        self.spicerack = spicerack
        self.datacenter: str = args.datacenter
        self.do_all: bool = args.all
        self.action: str = args.action
        self.task_id: str = args.task_id
        self.reason: Reason = spicerack.admin_reason(args.reason, task_id=self.task_id)
        self.catalog = self.spicerack.service_catalog()
        self.do_filter = args.filter
        self.discovery_records = self._get_all_services()
        self.insecure = args.fast_insecure
        # Stores the initial state of all services we've acted upon.
        # Used for rollbacks.
        self.initial_state: dict[str, set[str]] = {}
        self._recursors: RemoteHosts = spicerack.remote().query("A:dns-rec")
        self._authdns: RemoteHosts = spicerack.remote().query("A:dns-auth")
        if self.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None

    @property
    def runtime_description(self) -> str:
        """Used to log the action performed to SAL"""
        if self.do_all:
            services_msg = "all services"
        else:
            services_msg = "all active/active services"

        log_msg = f"{self.action} {services_msg} in {self.datacenter}: {self.reason.reason} - {self.reason.task_id}"
        # Indicate the cookbook is being run in emergency mode
        if self.insecure:
            log_msg = f"🤠EMERGENCY RUN🤠 {log_msg}"
        return log_msg

    def run(self):
        """Execute the desired action."""
        if self.action == "status":
            self.status()
            return
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id, f"{self.reason.owner} - Cookbook {__name__} {self.runtime_description} started."
            )
        pool = self.action == "pool"
        # For each A/A discovery record, check if the service is pooled in more than just the datacenter we're depooling
        # from.
        # If not, ask the user to choose between skipping it and moving it
        progress = 0
        total_records = sum(len(groups) for groups in self.discovery_records.values())
        for record in self.discovery_records["active_active"]:
            progress += 1
            logger.info("[%d/%d] Handling A/A service %s", progress, total_records, record.name)
            try:
                # Unpack the state tuple returned by _get_active_active_states
                (current_state, desired_state) = confirm_on_failure(self._get_active_active_states, record, pool)
                self._handle_active_active(record, current_state, desired_state)
            except TypeError:
                # confirm_on_failure returns None when the skip option is chosen. Since we try to unpack the tuple
                # returned by _get_active_active_states directly into variables, it throws a TypeError exception,
                # which we catch to log and skip this record without breaking out of the loop.
                logger.warning("Skipping %s", record.name)

        # For each A/P discovery record, first ensure we're pooling another core datacenter, then depool the current
        # one. We'll ask confirmation for each of them.
        for record in self.discovery_records["active_passive"]:
            progress += 1
            logger.info("[%d/%d] Handling A/P service %s", progress, total_records, record.name)
            try:
                # Unpack the state tuple returned by _get_active_passive_states
                (current_state, desired_state) = confirm_on_failure(self._get_active_passive_states, record, pool)
                self._handle_active_passive(record, current_state, desired_state)
            except TypeError:
                # confirm_on_failure returns None when the skip option is chosen. Since we try to unpack the tuple
                # returned by _get_active_active_states directly into variables, it throws a TypeError exception,
                # which we catch to log and skip this record without breaking out of the loop.
                logger.warning("Skipping %s", record.name)
        if self.insecure:
            self._clean_all()
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id, f"{self.reason.owner} - Cookbook {__name__} {self.runtime_description} completed."
            )

    def status(self):
        """Get service status in datacenter."""
        skipped = []
        if self.datacenter == "all":
            datacenters: tuple[str, ...] = CORE_DATACENTERS
        else:
            datacenters = (self.datacenter,)
        status = {}
        # Gather status information
        for datacenter in datacenters:
            for group in self.discovery_records.values():
                for record in group:
                    if record.name not in status:
                        status[record.name] = {"type": record.type}
                    try:
                        if record.is_pooled_in_dc(datacenter):
                            status[record.name][datacenter] = "pooled"
                        else:
                            status[record.name][datacenter] = ""
                    except ConfctlError:
                        logger.error("Can't fetch status for %s", record.name)
                        skipped.append(f"{record} {datacenter}")
                        status[record.name][datacenter] = "error"

        # Header setup for pretty printing
        dc_header_string = "".join([f"{dc:<10}" for dc in datacenters])
        header = f"{'Service':<30}{'Type':<15}{dc_header_string}"
        print(header)
        print("=" * len(header))

        for service in sorted(status.keys()):
            service_status_string = "".join([f"{status[service][dc]:<10}" for dc in datacenters])
            print(f"{service:<30}{status[service]['type']:<15}{service_status_string}")

        skipped.sort()
        if skipped:
            print("=== SKIPPED SERVICES ===")
            print("\n".join(skipped))

    def rollback(self):
        """Roll back everything we've done."""
        if self.action == "status":
            return
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id, f"{self.reason.owner} - Cookbook {__name__} {self.runtime_description} failed."
            )
        ask_confirmation("Do you wish to rollback to the state before the cookbook ran?")

        for record in self.discovery_records["active_passive"]:
            if record.name in self.initial_state:
                self._handle_active_passive(record, record.state, self.initial_state[record.name])

        for record in self.discovery_records["active_active"]:
            if record.name in self.initial_state:
                self._handle_active_active(record, record.state, self.initial_state[record.name])
        if self.insecure:
            self._clean_all()
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.task_id, f"{self.reason.owner} - Cookbook {__name__} {self.runtime_description} rolled back."
            )

    def _get_active_active_states(self, record: DiscoveryRecord, pool: bool):
        # Store the current state
        current_state = record.state
        self.initial_state[record.name] = current_state
        is_pooled = record.is_pooled_in_dc(self.datacenter)
        # now determine what is the desired state
        desired_state = current_state.copy()
        if pool and not is_pooled:
            desired_state.add(self.datacenter)
        if not pool and is_pooled:
            desired_state.remove(self.datacenter)
        return (current_state, desired_state)

    def _get_active_passive_states(self, record: DiscoveryRecord, pool: bool):
        # store the current state
        current_state = record.state
        self.initial_state[record.name] = current_state
        desired_state: set[str] = current_state.copy()
        if pool:
            # If we're pooling the current datacenter, we want it to be the only datacenter pooled in the end.
            desired_state = set(self.datacenter)
        else:
            # If depooling, we just remove the current datacenter from the desired state.
            # If desired_state ends up empty, _skip_or_move will be called.
            desired_state.discard(self.datacenter)
        return (current_state, desired_state)

    def _handle_active_active(self, record: DiscoveryRecord, current_state: set[str], desired_state: set[str]):
        if desired_state == current_state:
            logger.info("Service %s is already in the desired state", record.name)
            return
        # safety check: if the desired state would have the service depooled everywhere, refuse to do anything and go
        # away, unless the user provides a choice.
        if len(desired_state) == 0:
            logger.warning("The current operation would leave service %s completely depooled.", record.name)
            self._skip_or_move(record, self.datacenter)
            return

        # Now pool all services that are currently not pooled
        for datacenter in desired_state - current_state:
            record.pool(datacenter)

        # And depool all services that are currently pooled and shouldn't be
        for site in current_state - desired_state:
            record.depool(site)
        # Unless we're running in an emergency, check the records and clear the recursors cache
        if not self.insecure:
            record.check_records()
            record.clear_cache(self._recursors)

    def _handle_active_passive(self, record: DiscoveryRecord, current_state: set[str], desired_state: set[str]):
        if desired_state == current_state:
            logger.info("Service %s is already in the desired state", record.name)
            return

        # Check current_state size
        pooled_now = len(current_state)
        pooled_after = len(desired_state)
        # safety check: if the service is pooled in multiple datacenters, stop and request a manual solution
        if pooled_now > 1:
            ask_confirmation(
                f"The discovery record {record.name} is A/P but pooled in multiple datacenters. "
                "Please resolve manually using conftool and confirm resolution. The cookbook will "
                "NOT act on the service."
            )
            return

        if pooled_after > 1:
            logger.error("We cannot pool %s in multiple datacenters. Ignoring the request.", record.name)
            return

        if pooled_now == 0:
            # The service is unpooled. We just assign a random dc_from here.
            dc_from = "none"
        else:
            dc_from = list(current_state).pop()

        if pooled_after == 0:
            dc_to = None
        else:
            dc_to = list(desired_state).pop()

        self._skip_or_move(record, dc_from, dc_to)

    def _clean_all(self):
        """Wipe all caches twice."""
        # If running in an emergency: don't check the records, rather send two cache
        # purges for "discovery.wmnet" 30 seconds apart to all recursive DNS servers.
        # Then handle cleaning up for a/p services templates.
        self._recursors.run_sync("sudo rec_control wipe-cache discovery.wmnet")
        logger.info("Sleeping 30 seconds before sending a second wipe-cache command")
        time.sleep(30)
        self._recursors.run_sync("sudo rec_control wipe-cache discovery.wmnet")
        logger.info("==> Traffic should be fully migrated now <==")
        for record in self.discovery_records["active_passive"]:
            record.clean_discovery_templates(self._authdns)

    def _get_all_services(self) -> dict[str, list[DiscoveryRecord]]:
        all_services: dict[str, list[DiscoveryRecord]] = {"active_active": [], "active_passive": []}
        # We exclude:
        # - mediawiki read/write endpoints
        # - services that need special handling for switchover
        # - all active/passive services unless explicitly asked
        # - services that are not in lvs_state "production"
        for service in self.catalog:
            if service.discovery is None:
                logger.debug("Skipping %s, as it doesn't have a discovery record", service.name)
                continue
            if service.state != "production":
                logger.debug("Skipping %s, as its state %s is not production", service.name, service.state)
                continue
            if service.name in EXCLUDED_SERVICES and self.do_filter:
                logger.info("Skipping excluded service %s: %s", service.name, EXCLUDED_SERVICES[service.name])
                continue
            for record in service.discovery:
                complete_record = DiscoveryRecord(record=record, ips=service.ip)
                if complete_record.name in MEDIAWIKI_SERVICES and self.do_filter:
                    logger.info("Skipping %s, (use the sre.switchdc.mediawiki cookbook instead)", complete_record.name)
                    continue
                # Do not add active/passive services if do_all is not selected.
                if complete_record.active_active:
                    all_services["active_active"].append(complete_record)
                elif self.do_all:
                    all_services["active_passive"].append(complete_record)
        return all_services

    def _skip_or_move(self, record: DiscoveryRecord, dc_from: str, dc_to=None):
        # For services pooled only in the datacenter we're depooling from, skip them or move them based on user input.
        available_dcs = [dc for dc in record.ips.sites if dc != dc_from]
        selected = None
        if dc_to is not None:
            dest = dc_to
            selected = dc_to
        else:
            dest = "another datacenter"
            if len(available_dcs) == 1:
                selected = available_dcs.pop()
                dest = selected

        try:
            action = ask_input(
                f"{record.fqdn} is only pooled in {dc_from}: skip or move to {dest}?",
                ["move", "skip"],
            )
            if action == "skip":
                return

            if selected is None:
                selected = ask_input("Please pick a datacenter to move to", available_dcs)
        except InputError:
            logger.error("Invalid responses, NOT acting on record %s", record.fqdn)
            return

        record.pool(selected)
        # If we're pooling a previously completely depooled service, we don't need
        # to depool anything.
        if dc_from in CORE_DATACENTERS:
            record.depool(dc_from)
        # If not running in an emergency, do all the checks here.
        if not self.insecure:
            record.check_records()
            record.clean_discovery_templates(self._authdns)
            record.clear_cache(self._recursors)
