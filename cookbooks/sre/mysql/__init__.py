"""Mysql cookbooks.

Shared library-functionality is present in this file
to help reorganise and reduce duplication. Most should
be moved to spicerack.mysql, once mature.
"""

__owner_team__ = "Data Persistence"

import logging
from abc import (
    ABCMeta,
    abstractproperty,
)
from argparse import (
    ArgumentParser,
    Namespace,
)
from configparser import ConfigParser
from enum import Enum
from os import getenv

from pydantic import (
    BaseModel,
    Field,
)
from spicerack import Spicerack
from spicerack.apiclient import APIClient
from spicerack.cookbook import (
    CookbookBase,
    CookbookRunnerBase,
)
from spicerack.mysql import MysqlRemoteHosts
from spicerack.remote import (
    RemoteExecutionError,
    RemoteHosts,
)
from wmflib.interactive import (
    AbortError,
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
)

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


## WIP - cleanup process for all cookbooks, starting with T433044


class YesNo(Enum):
    """Truthy type."""

    NO = False
    YES = True

    @classmethod
    def _missing_(cls, value):
        """Called automatically when value doesn't match a member's raw value."""
        if isinstance(value, str):
            value = value.strip().lower()
        if value in (1, "1", "y", "yes", "true"):
            return cls.YES
        if value in (0, "0", "n", "no", "false"):
            return cls.NO
        return None  # Enum turns this into a ValueError for us

    def __bool__(self) -> bool:
        return self.value

    def __str__(self) -> str:
        return self.name.lower()


class TooManyMySQLHostsError(ValueError):
    """Indicate that the number of MySQL hosts is exceeding a threshold"""


class MySQLCookbookBase(CookbookBase, metaclass=ABCMeta):
    """Base cookbook class for sre.mysql."""

    argument_reason_required = True
    argument_task_required = True

    def argument_parser(self) -> ArgumentParser:
        """CLI parsing, as required by the Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "query",
            help="Cumin query to match the host(s) to act upon.",
        )
        return parser


class MySQLCookbookRunnerBase(CookbookRunnerBase, metaclass=ABCMeta):
    """Base cookbook runner class for sre.mysql."""

    CAUTIOUS_MODE: YesNo = YesNo(getenv("COOKBOOK_SRE_MYSQL_CAUTIOUS_MODE", "1"))

    hosts: RemoteHosts
    log: logging.Logger

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Set things up"""
        ensure_shell_is_durable()

        self.spicerack = spicerack

        self.args = args
        self.hosts = self.get_remotehosts()
        self.icinga_hosts = spicerack.icinga_hosts
        self.alerting_hosts = spicerack.alerting_hosts
        self.log = logging.getLogger(__name__)
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        self.puppet = spicerack.puppet

    @abstractproperty
    def max_hosts(self) -> int:
        """Control the maximum host count for the runner.

        Provide per-cookbook control over the acceptable host count for a run.

        0 - use to provide unlimited hosts
        1 - this should be the default, limiting execution to a single host
        n - a positive integer greater than 1 should be used with care
        """

    @max_hosts.setter
    def max_hosts(self, val) -> None:
        """Prevent changes to the max_hosts"""
        raise AttributeError("max_hosts is read-only")

    @max_hosts.deleter
    def max_hosts(self) -> None:
        """Prevent deletion of max_hosts"""
        raise AttributeError("max_hosts is read-only")

    @abstractproperty
    def permitted_sections(self) -> list:
        """Provide a list of permitted sections."""

    @permitted_sections.setter
    def permitted_sections(self, val) -> None:
        """Prevent changes to the max_hosts"""
        raise AttributeError("max_hosts is read-only")

    @permitted_sections.deleter
    def permitted_sections(self) -> None:
        """Prevent deletion of max_hosts"""
        raise AttributeError("max_hosts is read-only")

    def ensure(self, condition: bool, msg: str, error_cls: type[Exception] = AssertionError) -> None:
        """Just some syntactic sugar for readability."""
        if condition:
            return
        self.log.error("Failed safety check: {msg}", exc_info=True)
        raise error_cls(msg)

    def get_mysqlremotehosts(self, fqdn: str) -> MysqlRemoteHosts:
        """Returns a `MysqlRemoteHosts` instance for a single, non multiinstance host or raises if not found"""
        mrhs: MysqlRemoteHosts = self.spicerack.mysql().get_dbs(
            "P{"
            + fqdn
            + "} and A:db-all and not A:db-multiinstance and ("
            + " or ".join([f"A:db-section-{x}" for x in self.permitted_sections])
            + ")"
        )
        self.ensure(len(mrhs) == 1, f"{len(mrhs)} Mysql instances found, expected one.")
        return mrhs

    def get_remotehosts(self) -> RemoteHosts:
        """Returns a RemoteHosts matching the query"""
        rhs: RemoteHosts = self.spicerack.remote().query("P{" + self.args.query + "} and A:db-all")
        if self.max_hosts != 0:
            self.ensure(
                len(rhs) <= self.max_hosts, f"{len(rhs)} remote instances found, expected {self.max_hosts} or less."
            )
        return rhs

    def check_sections(self) -> bool:
        """Check that the chosen hosts are all in the permitted sections

        Raises: spicerack.remote.RemoteError if failing to match

        """
        self.log.info("Hosts = %s", self.hosts)
        for host in self.hosts:
            self.log.info("Host: %s", host)
            self.get_mysqlremotehosts(fqdn=str(host))
        return len(self.hosts) > 0

    def run(self) -> None:
        """Required by the Spicerack API."""
        logging.getLogger("conftool").setLevel(logging.WARNING)

    def run_sync_single(self, host, command: str, ensure_green: bool = True) -> None:
        """Run a synchronous command across a single host."""
        self.ensure(len(host.hosts) == 1, "A single MySQL instance is required", TooManyMySQLHostsError)
        try:
            if bool(self.CAUTIOUS_MODE):
                ask_confirmation(f"Proceed running {command} against {host}?")
            self.log.info("Executing %s on %s", command, host)
            confirm_on_failure(host.run_sync, command)
        except RemoteExecutionError as err:
            for _, mt in err.results:
                self.log.debug("Unexpected output: %s", mt)
        except AbortError:
            self.log.error("%s: execution aborted", command)
            raise
        if ensure_green:
            self.icinga_hosts(host.hosts).wait_for_optimal()

    def run_sync_all(self, command: str, ensure_green: bool = True) -> None:
        """Run a synchronous command across all hosts."""
        for host in self.hosts:
            self.run_sync_single(host=host, command=command, ensure_green=ensure_green)

    @staticmethod
    def _load_replication_user_password() -> tuple[str, str]:
        my_cnf = ConfigParser()
        my_cnf.read("/root/.my.cnf")
        clientreplication = my_cnf["clientreplication"]
        return clientreplication["user"], clientreplication["password"]


## WIP cleanup process for existing cookbooks

log = logging.getLogger(__name__)


def ensure(condition: bool, msg: str) -> None:
    """Just some syntactic sugar for readability."""
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def get_mysqlremotehosts(spicerack, fqdn: str) -> MysqlRemoteHosts:
    """Returns a `MysqlRemoteHosts` instance for a single, non multiinstance host or raises if not found"""
    query = "P{" + fqdn + "} and A:db-all and not A:db-multiinstance"
    mrhs: MysqlRemoteHosts = spicerack.mysql().get_dbs(query)
    ensure(len(mrhs) == 1, f"{len(mrhs)} Mysql instances found, expected one")
    return mrhs


# # APIs # #


# class ConfigMasterMW(BaseModel):
#     primary_dc: Literal["codfw", "eqiad"]


# def fetch_primary_dc() -> Literal["codfw", "eqiad"]:
#     url = "https://config-master.wikimedia.org/mediawiki.yaml"
#     log.debug(f"Fetching {url}")
#     with urlopen(url, timeout=5) as resp:
#         y = ConfigMasterMW.model_validate(yaml.safe_load(resp))

#     return y.primary_dc


# # zarcillo # #


class InstanceMetadata(BaseModel):
    dc: str
    fqdn: str
    hostname: str
    instance_group: str
    instance_name: str
    port: int
    section: str
    role: str
    preferred_candidate: bool
    candidate_score: int = 0


class SectionStatus(BaseModel):
    name: str
    instance_cnt: int
    instances: list[InstanceMetadata] = Field(default_factory=list)
    hp: int | None = None


class ZarcilloClient:
    # TODO: use httpx ideally
    def __init__(self, spicerack: Spicerack):
        self._client: APIClient = spicerack.api_client("https://zarcillo.wikimedia.org/", tries=5)
        self._client.http_session.headers.update({"X-WMF-Username": spicerack.username})

    def fetch_section_status(self, section_name: str) -> SectionStatus:
        path = f"/api/v1/section_status/{section_name}"
        resp = self._client.request("GET", path)
        resp.raise_for_status()
        j = resp.json()
        if "name" not in j or "instances" not in j:
            raise RuntimeError(f"Unexpected response {j}")

        return SectionStatus(**j)
