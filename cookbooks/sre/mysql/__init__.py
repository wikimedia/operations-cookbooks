"""Mysql cookbooks."""

__owner_team__ = "Data Persistence"

import logging

from pydantic import BaseModel, Field
from spicerack import Spicerack
from spicerack.apiclient import APIClient
from spicerack.mysql import MysqlRemoteHosts

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
