"""Pool/repool liberica in one or multiple hosts."""
import argparse
import logging
import time
from datetime import datetime, timedelta

from prometheus_client.parser import text_string_to_metric_families

from spicerack import Spicerack, Reason
from spicerack.decorators import retry
from spicerack.remote import RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)


class LibericaAdmin(SREBatchBase):
    r"""Perform liberica admin actions

    Example usage:
        cookbook sre.loadbalancer.admin --query 'P{lvs4010.ulsfo.wmnet}' --reason "LB maintenance" depool
        cookbook sre.loadbalancer.admin --alias 'liberica-canary' \
                --reason "deploy new service" config_reload

    """

    batch_default = 1
    batch_max = 1
    valid_actions = ("pool", "depool", "config_reload")

    def get_runner(self, args: argparse.Namespace):
        """Get the worker class."""
        return LibericaAdminRunner(args, self.spicerack)


class LibericaAdminRunner(SREBatchRunnerBase):
    """Controller class for Liberica admin operations"""

    def __init__(self, args: argparse.Namespace, spicerack: Spicerack) -> None:
        """Initializes the parent class, also adds an http session"""
        super().__init__(args, spicerack)
        self._http = spicerack.requests_session(__name__, timeout=5.0, tries=3, backoff=2.0)
        self._reload_ts = int(time.time())
        reason = "sre.loadbalancer.admin (de)pooling in progress"
        if args.task_id:
            reason += f" ({args.task_id})"
        self._puppet_reason = spicerack.admin_reason(reason)

    @property
    def allowed_aliases(self) -> list:
        """List of allowed aliases for host selection"""
        base_aliases = ["liberica", "liberica-canary"]
        all_aliases = []
        for base in base_aliases:
            all_aliases.append(base)
            for datacenter in ALL_DATACENTERS:
                all_aliases.append(f"{base}-{datacenter}")

        return all_aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Helper property to return a cumin formatted query of allowed aliases"""
        return "A:liberica"

    @property
    def runtime_description(self) -> str:
        """pretty-print message"""
        msg = f"{self._args.action}ing {self._query()}"
        if self._args.task_id:
            msg += f" ({self._args.task_id})"
        return msg

    def _depool_action(self, hosts: RemoteHosts, _: Reason) -> None:
        """Depool liberica instance by stopping the control plane service"""
        puppet = self._spicerack.puppet(hosts)
        puppet.disable(self._puppet_reason, verbatim_reason=True)
        depool_cmd = "/bin/systemctl stop liberica-cp.service"
        confirm_on_failure(hosts.run_sync, depool_cmd)

    def _pool_action(self, hosts: RemoteHosts, _: Reason) -> None:
        """Pool liberica instance by starting the control plane service"""
        puppet = self._spicerack.puppet(hosts)
        pool_cmd = "/bin/systemctl start liberica-cp.service"
        confirm_on_failure(hosts.run_sync, pool_cmd)
        puppet.enable(self._puppet_reason, verbatim_reason=True)

    def _config_reload_action(self, hosts: RemoteHosts, _: Reason) -> None:
        reload_cmd = "/bin/systemctl reload liberica-cp.service"
        confirm_on_failure(hosts.run_sync, reload_cmd)

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Raise a RuntimeError if the instance isn't on the expected state"""
        if self._args.action == "config_reload":
            return

        expect_pooled = self._args.action == "depool"
        self._validate_is_pooled(hosts, expect_pooled)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Raise a RuntimeError if the instance isn't on the expected state"""
        if self._args.action == "config_reload":
            self._validate_succesful_config_reload(hosts)
            return

        expect_pooled = self._args.action == "pool"
        self._validate_is_pooled(hosts, expect_pooled)

    @retry(tries=15, delay=timedelta(seconds=3), backoff_mode="constant", exceptions=(RuntimeError,))
    def _validate_is_pooled(self, hosts: RemoteHosts, expect_pooled: bool) -> None:
        for fqdn in hosts.hosts:
            pooled = self._is_pooled(fqdn)
            if pooled != expect_pooled:
                raise RuntimeError(f"Unexpected pooled state on {fqdn}, want {expect_pooled}, got {pooled}")

    def _is_pooled(self, host: str) -> bool:
        """Fetch gobgp metrics and check the number of advertised routes and peers"""
        logger.info("Checking BGP status on %s", host)
        response = self._http.get(f"http://{host}:3010/metrics")

        established = 0
        advertised = 0
        for metric in text_string_to_metric_families(response.text):
            if metric.name == "bgp_peer_state":
                for sample in metric.samples:
                    if sample.labels.get("admin_state") == "UP" and sample.labels.get("session_state") == "ESTABLISHED":
                        logger.debug("found BGP session with %s", sample.labels.get("peer"))
                        established += 1
            elif metric.name == "bgp_routes_advertised":
                for sample in metric.samples:
                    if sample.labels.get("peer") and sample.labels.get("route_family") and int(sample.value) > 0:
                        logger.debug("found %d %s BGP routes advertised with %s",
                                     sample.value, sample.labels.get("route_family"), sample.labels.get("peer"))
                        advertised += int(sample.value)

        if established and advertised:
            return True

        logger.debug("no BGP advertised routes found")
        return False

    @retry(tries=3, delay=timedelta(seconds=3), backoff_mode="constant", exceptions=(RuntimeError,))
    def _validate_succesful_config_reload(self, hosts: RemoteHosts) -> None:
        for fqdn in hosts.hosts:
            last_reload = self._successful_config_reload(fqdn)
            if last_reload < self._reload_ts:
                dt_last_reload = datetime.fromtimestamp(last_reload)
                dt_reload_after = datetime.fromtimestamp(self._reload_ts)
                raise RuntimeError(f"latest config reload on {fqdn}: {dt_last_reload} < {dt_reload_after}")

    def _successful_config_reload(self, host: str) -> int:
        """Fetch the timestamp of the latest successful config reload performed by liberica control plane"""
        logger.info("validating control plane configuration got reloaded successfully on %s", host)
        response = self._http.get(f"http://{host}:3003/metrics")
        for metric in text_string_to_metric_families(response.text):
            if metric.name == "liberica_cp_configuration_reload_timestamp_seconds":
                for sample in metric.samples:
                    if sample.labels.get("result") == "ok":
                        return int(sample.value)

        raise RuntimeError(f"unable to find a succesful config reload on {host}")
