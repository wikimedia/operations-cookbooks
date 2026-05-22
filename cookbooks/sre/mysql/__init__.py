"""Mysql cookbooks.

Shared library-functionality is present in this file
to help reorganise and reduce duplication. Most should
be moved to spicerack.mysql, once mature.
"""

__owner_team__ = "Data Persistence"

import logging
from abc import (
    ABCMeta,
    abstractmethod,
)
from argparse import (
    ArgumentParser,
    Namespace,
)
from collections.abc import Callable
from configparser import ConfigParser
from datetime import datetime, timedelta
from enum import Enum
from os import getenv
from typing import Any

import phabricator
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
)
from spicerack import Spicerack
from spicerack.apiclient import APIClient
from spicerack.cookbook import (
    CookbookBase,
    CookbookRunnerBase,
)
from spicerack.decorators import retry
from spicerack.mysql import Instance as MInst
from spicerack.mysql import MysqlRemoteHosts
from spicerack.remote import (
    RemoteError,
    RemoteExecutionError,
    RemoteHosts,
)
from wmflib.interactive import (
    AbortError,
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
)

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

    @property
    @abstractmethod
    def max_hosts(self) -> int:
        """Control the maximum host count for the runner.

        Provide per-cookbook control over the acceptable host count for a run.

        0 - use to provide unlimited hosts
        1 - this should be the default, limiting execution to a single host
        n - a positive integer greater than 1 should be used with care
        """

    @property
    @abstractmethod
    def permitted_sections(self) -> list:
        """Provide a list of permitted sections."""

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

    # occasional "spicerack.remote.RemoteError: No hosts provided" has been raised
    @retry(tries=20, delay=timedelta(seconds=5), backoff_mode="constant", exceptions=(RemoteError,))
    def get_minst(self, hostname: str) -> MInst:
        """Retrieves a `spicerack.mysql` `Instance` as `MInst`
        ensuring it's just one instance.
        """
        query = "P{%s*} and A:db-all and not A:db-multiinstance" % hostname
        mrhs: MysqlRemoteHosts = self.spicerack.mysql().get_dbs(query)
        ensure(len(mrhs) == 1, f"{len(mrhs)} Mysql instances found, expected one")
        instances: list[MInst] = mrhs.list_hosts_instances()
        return instances[0]


## WIP cleanup process for existing cookbooks


# # generic helpers # #

log = logging.getLogger(__name__)


def ensure(condition: bool, msg: str) -> None:
    """Just some syntactic sugar for readability."""
    if condition:
        return
    log.error("Failed safety check: {msg}", exc_info=True)
    raise AssertionError(msg)


def init_step(module_name: str) -> Callable[[str, str], None]:
    """
    Log next step in a friendly/greppable format.
    """
    # TODO: store the step in zarcillo to create visibility around the automation process
    # TODO: also log msg in open telemetry format for tracing

    def step(slug: str, msg: str) -> None:
        log.info("[%s.%s] %s", module_name, slug, msg)

    return step


# # Spicerack helpers # #


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


class InstancesMetadataResp(BaseModel):
    instances: list[InstanceMetadata]


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

    def fetch_instances_meta(self, hostname: str) -> list[InstanceMetadata]:
        """Fetch instances on a host /api/v1/instances/{hostname}"""
        path = f"/api/v1/instances/{hostname}"
        resp = self._client.request("GET", path)
        resp.raise_for_status()

        payload = InstancesMetadataResp.parse_obj(resp.json())
        return payload.instances


# # Phabricator # #


class PhabTask(BaseModel):
    author_phid: str | None = None
    date_created: datetime | None = None
    date_modified: datetime | None = None
    description: str
    id: int
    is_closed: bool = False
    owner_phid: str | None = None
    phid: str
    priority: str | None = None
    project_phids: list[str] = Field(default_factory=list)
    status: str | None = None
    title: str

    # # internals # #

    _client: phabricator.Phabricator = PrivateAttr()
    _parent = PrivateAttr()
    _pending_txns: list[dict[str, Any]] = PrivateAttr(default_factory=list)

    @classmethod
    def from_api_dict(cls, task_data: dict[str, Any], client: phabricator.Phabricator, parent) -> "PhabTask":
        fields = task_data.get("fields", {})
        raw_desc = fields.get("description", {}).get("raw", "")

        status_data = fields.get("status", {})
        priority_data = fields.get("priority", {})

        out = cls(
            author_phid=fields.get("authorPHID"),
            date_created=fields.get("dateCreated"),
            date_modified=fields.get("dateModified"),
            description=raw_desc,
            id=task_data["id"],
            is_closed=fields.get("isClosed", False),
            owner_phid=fields.get("ownerPHID"),
            phid=task_data["phid"],
            priority=priority_data.get("name") if isinstance(priority_data, dict) else None,
            project_phids=fields.get("projectPHIDs", []),
            status=status_data.get("value") if isinstance(status_data, dict) else None,
            title=fields.get("name", fields.get("title", "")),
        )
        out._client = client
        out._parent = parent
        return out

    # # transaction methods # #

    def unassign(self) -> "PhabTask":
        self._pending_txns.append({"type": "owner", "value": None})
        return self

    def add_project_tags(self, tags: list[str]) -> "PhabTask":
        project_phids = self._parent.lookup_project_phids(tags)
        self._pending_txns.append({"type": "projects.add", "value": project_phids})
        return self

    def remove_project_tags(self, tags: list[str]) -> "PhabTask":
        project_phids = self._parent.lookup_project_phids(tags)
        self._pending_txns.append({"type": "projects.remove", "value": project_phids})
        return self

    def set_project_tags(self, tags: list[str]) -> "PhabTask":
        """Set project tags e.g. #DBA removing any previous one"""
        project_phids = self._parent.lookup_project_phids(tags)
        self._pending_txns.append({"type": "projects.set", "value": project_phids})
        return self

    def add_comment(self, text: str) -> "PhabTask":
        self._pending_txns.append({"type": "comment", "value": text})
        return self

    def assign_to(self, username: str) -> "PhabTask":
        phid = self._parent.lookup_user_phid(username)
        self._pending_txns.append({"type": "owner", "value": phid})
        return self

    def set_description(self, description: str) -> "PhabTask":
        self._pending_txns.append({"type": "description", "value": description})
        return self

    def send(self) -> Any:
        """Send all changes in an API call"""
        if not self._pending_txns:
            return

        out = self._client.maniphest.edit(
            objectIdentifier=self.phid,
            transactions=self._pending_txns,
        )
        self._pending_txns.clear()
        return out


class Phabricator:
    """Phabricator client wrapper

    Example:

        >>> phab = phabricator_client(spicerack)
        >>> t = phab.fetch_task("T434788")
        >>> print(t.status)
        >>> print(t.title)
        >>> print(t.description)
        >>> res = t.add_comment("comment").assign_to("JDoe-WMF").add_project_tags(["#DBA"]).send()
        >>> print(res.transactions)
    """

    # TODO: move into wmflib

    def __init__(self, phab) -> None:
        self.phab = phab

    @property
    def _client(self):
        return self.phab._client

    def task_accessible(self, task_id: str, *, raises: bool = True) -> bool:
        return self.phab.task_accessible(task_id, raises=raises)

    def task_comment(self, task_id: str, comment: str, *, raises: bool = True) -> None:
        self.phab.task_comment(task_id, comment, raises=raises)

    def fetch_task(self, task_id: str) -> PhabTask:
        """Fetch task by ID e.g. T1234 and return a PyDantic obj with the most useful fields"""
        task_int = int(task_id.strip("T"))
        task_res = self._client.maniphest.search(constraints={"ids": [task_int]})
        if not task_res["data"]:
            raise ValueError(f"Task {task_id} not found.")

        if len(task_res["data"]) != 1:
            raise ValueError("Expected only one item in `data`")

        return PhabTask.from_api_dict(
            task_res["data"][0],
            self._client,
            self,
        )

    def lookup_project_phids(self, slugs: list[str]) -> list[str]:
        resp = self._client.project.search(constraints={"slugs": slugs})
        return [p["phid"] for p in resp["data"]]

    def lookup_user_phid(self, username: str) -> str:
        username = username.lstrip("@")
        resp = self._client.user.search(constraints={"usernames": [username]})
        if not resp.get("data"):
            raise ValueError(f"User '{username}' not found.")

        return resp["data"][0]["phid"]


def phabricator_client(spicerack: Spicerack) -> Phabricator:
    wph = spicerack.phabricator("/etc/phabricator_ops-monitoring-bot.conf")
    return Phabricator(wph)
