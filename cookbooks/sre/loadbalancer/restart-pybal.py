"""Restart pybal on one or multiple hosts."""
import argparse
from collections import defaultdict
import logging
import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, List

from spicerack import Spicerack
from spicerack.decorators import retry
from spicerack.exceptions import SpicerackCheckError
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS, CORE_DATACENTERS
from wmflib.requests import http_session

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class BGPSessionMetric:
    """Simple representation of a bgp metric from pybal"""

    host: str
    asn: str
    peer: str
    status: bool


class BGPSessionError(SpicerackCheckError):
    """BGP session specific exception"""

    @classmethod
    def from_metrics(cls, metrics: List[BGPSessionMetric]) -> "BGPSessionError":
        """Factory method to create the exception from a metric."""
        # All metrics share the host and asn properties, so just pick the first.
        metric = metrics[0]
        peers = ", ".join([m.peer for m in metrics])
        return cls(f"{metric.host} - AS {metric.asn}, peer {peers} session down")


class LBRestart(SREBatchBase):
    """Controller class for LB restarts"""

    # We could in theory be fancy, but let's
    # play it safe and say we can only restart load-balancers one at a time.
    batch_default = 1
    batch_max = 1
    valid_actions = ("restart-daemons",)

    def get_runner(self, args: argparse.Namespace):
        """Get the worker class."""
        return LBRestartRunner(args, self.spicerack)


class LBRestartRunner(SREBatchRunnerBase):
    """LB restarts worker class"""

    def __init__(self, args: argparse.Namespace, spicerack: Spicerack) -> None:
        """Initializes the parent class, also adds an http session"""
        super().__init__(args, spicerack)
        self._http = http_session("PybalRestart", timeout=2.0, tries=2, backoff=2.0)

    @property
    def allowed_aliases(self) -> List:
        """List of allowed aliases for host selection"""
        base_aliases = ["lvs", "lvs-high-traffic1", "lvs-high-traffic2", "lvs-secondary"]
        core_aliases = ["lvs-low-traffic"]
        all_aliases = []
        for base in base_aliases:
            all_aliases.append(base)
            for datacenter in ALL_DATACENTERS:
                all_aliases.append(f"{base}-{datacenter}")
        for base in core_aliases:
            all_aliases.append(base)
            for datacenter in CORE_DATACENTERS:
                all_aliases.append(f"{base}-{datacenter}")
        return all_aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Helper property to return a cumin formatted query of allowed aliases"""
        return "A:lvs"

    @property
    def restart_daemons(self) -> List:
        """Property to return a list of daemons to restart"""
        return ["pybal.service"]

    @property
    def runtime_description(self) -> str:
        """pretty-print message"""
        msg = f"rolling-restart of pybal on {self._query()}"
        if self._args.task_id:
            msg += f" ({self._args.task_id})"
        return msg

    def post_action(self, hosts: RemoteHosts) -> None:
        """The method is executed after every batch is executed.

        Query the load-balancer to find the status of the bgp sessions.
        """
        for fqdn in hosts.hosts:
            logger.info("Checking BGP sessions on %s", fqdn)
            self._check_bgp_sessions(fqdn)

    @retry(tries=10, delay=timedelta(seconds=10), backoff_mode="constant", exceptions=(BGPSessionError,))
    def _check_bgp_sessions(self, fqdn: str):
        per_asn_results = self._fetch_and_parse_metrics(fqdn)
        for statuses in per_asn_results.values():
            if not any(s.status for s in statuses):
                raise BGPSessionError.from_metrics(statuses)

    def _fetch_and_parse_metrics(self, host: str) -> Dict[str, BGPSessionMetric]:
        """Fetch the /metrics endpoint on the host, then check all the pybal_bgp_session_established ones.

        We need to perform the test this way as thanos queries would not
        have the needed time sensitivity. Also, this removes any dependency
        on thanos for this cookbook, which is good if you keep in mind thanos is
        beyond a load-balancer.
        """
        response = self._http.get(f"http://{host}:9090/metrics")
        reg = re.compile(r"^pybal_bgp_session_established\{local_asn=\"(\d+)\",peer=\"(.+?)\"\} (.+)$")
        results = defaultdict(list)
        for line in response.text.splitlines():
            match = reg.match(line)
            if match is None:
                continue
            asn, peer, status = match.groups()
            results[asn].append(BGPSessionMetric(host, asn, peer, status == "1.0"))
        return results
