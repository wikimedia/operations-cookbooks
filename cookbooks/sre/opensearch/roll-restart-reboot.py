"""Perform rolling operations on opensearch servers"""

import logging
import socket
from argparse import ArgumentParser, Namespace
from contextlib import contextmanager
from datetime import timedelta
from typing import Optional

from requests.exceptions import RequestException
from spicerack import Spicerack
from spicerack.apiclient import APIClientResponseError
from spicerack.confctl import ConfctlError
from spicerack.decorators import retry
from spicerack.remote import RemoteError, RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)
CLUSTERGROUPS = ("datahubsearch", "logstash-eqiad", "logstash-codfw")


class ClusterHealthNotGreen(Exception):
    """Exception raised when an OpenSearch cluster health status is *not* green"""


class RollingOperation(SREBatchBase):
    """Perform a rolling operation on servers of an opensearch cluster.

      Process:

      1. Identify current main (elected master) node
      2. Restrict shard allocation to primaries only
      3. Perform restart/reboot action on single host
      4. After host rejoins, re-enable allocation of all shards
      5. Wait for the cluster to go green before moving to the next host
      6. The main (elected master) node is acted on last,
         to avoid triggering repeated leader elections
      7. Once every node is back and joined, allocation is left enabled

    Usage examples:
        cookbook sre.opensearch.roll-restart-reboot \
            --alias datahubsearch \
            --reason "Rolling reboot to pick up new kernel" \
            reboot

        cookbook sre.opensearch.roll-restart-reboot \
            --alias datahubsearch \
            --reason "Rolling restart to pick new OpenSSL" \
            restart_daemons
    """

    grace_sleep = 120
    min_grace_sleep = 60
    batch_max = 1

    def argument_parser(self) -> ArgumentParser:
        """Add cookbook-specific arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--without-shard-allocation",
            action="store_true",
            help="Skip toggling shard allocation (primaries/all) around "
                 "each host action",
        )
        return parser

    # We must implement this abstract method
    def get_runner(self, args: Namespace):
        """As specified by Spicerack API."""
        return RollingOperationRunner(args, self.spicerack)


class RollingOperationRunner(SREBatchRunnerBase):
    """Apply rolling operation to cluster."""

    depool_sleep = 20

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Required by SREBatchRunnerBase"""

        self._main_node: Optional[str] = None
        self._confctl = spicerack.confctl("node")
        self._current_host_is_collector = False
        super().__init__(args, spicerack)

    def _hosts(self) -> list[RemoteHosts]:
        """Split the target hosts so the main/master node is at the end"""

        (hosts,) = super()._hosts()
        main = self.get_main_node(hosts)
        non_main = [host for host in hosts.hosts if host != main]

        remote = self._spicerack.remote()
        groups = []
        if non_main:
            groups.append(remote.query("D{" + ",".join(non_main) + "}"))
        groups.append(remote.query(f"D{{{main}}}"))

        logger.info("(%d) non-main nodes to be acted on first: %s",
                    len(non_main), non_main)
        logger.info(
            "Main node (elected master) to be acted on last is: %s", main)
        return groups

    @retry(
        tries=360,
        delay=timedelta(seconds=60),
        backoff_mode="constant",
        exceptions=(ClusterHealthNotGreen, RequestException, APIClientResponseError),
    )
    def check_for_green_indices(self) -> None:
        """Make sure the cluster indices are all green and settled (no
        shards relocating) before proceeding"""

        main = self._main_node
        client = self._spicerack.api_client(f"http://{main}:9200")
        resp = client.request("GET", "/_cluster/health")
        data = resp.json()
        if data["status"] != "green" or data["relocating_shards"]:
            raise ClusterHealthNotGreen(
                f"{main} reports cluster status: {data['status']}: "
                f"initializing={data['initializing_shards']} "
                f"relocating={data['relocating_shards']} "
                f"unassigned={data['unassigned_shards']} waiting for shard movement to settle")
        logger.info("Cluster status green and shard movement has settled, proceeding")



    def get_main_node(self, hosts: RemoteHosts) -> str:
        """Identify (and cache) current main/elected-master node"""

        if self._main_node is None:
            last_error: Optional[Exception] = None
            for host in hosts.hosts:
                try:
                    client = self._spicerack.api_client(f"http://{host}:9200")
                    resp = client.request("GET", "/_cat/master?format=json")
                    data = resp.json()[0]
                except (RequestException, APIClientResponseError, IndexError) as error:
                    logger.warning(
                        "Could not get main node from %s: %s",
                        host, error)
                    last_error = error
                    continue
                self._main_node = self._match_fqdn(hosts, data["host"])
                logger.info(
                    "Identified current main node (elected master): %s (via %s)",
                    self._main_node, host)
                break
            else:
                raise last_error or RequestException(
                    "No hosts were reachable to identify the main node")
        return self._main_node

    @staticmethod
    def _match_fqdn(hosts: RemoteHosts, reported_host: str) -> str:
        """Match a host reported by OpenSearch back to FQDN"""

        for host in hosts.hosts:
            if host == reported_host or host.split(".")[0] == reported_host:
                return host
        for host in hosts.hosts:
            try:
                if socket.gethostbyname(host) == reported_host:
                    return host
            except socket.gaierror:
                continue
        raise RuntimeError(
            f"Could not match main node host {reported_host!r} to a "
            "known host")

    def _is_collector(self, host: str) -> bool:
        """Check if host has the role::logging::opensearch::collector
        Puppet class applied"""

        query = f"C:role::logging::opensearch::collector and {host}"
        try:
            self._spicerack.remote().query(query)
            return True
        except RemoteError:
            logger.info("%s did not match %r", host, query)
            return False

    @retry(
        tries=60,
        delay=timedelta(seconds=10),
        backoff_mode="constant",
        exceptions=(RequestException, APIClientResponseError),
    )
    def set_shard_allocation(self, mode: str) -> None:
        """Set cluster.routing.allocation.enable via the cluster settings API.

        :param mode: "primaries" to only allow primary shard allocation
                      or "all" to re-allow allocation of every shard
        """

        main = self._main_node
        client = self._spicerack.api_client(f"http://{main}:9200")
        client.request(
            "PUT", "/_cluster/settings",
            json={"persistent": {"cluster.routing.allocation.enable": mode}},
        )
        if self._spicerack.dry_run:
            logger.info(
                "DRY-RUN: would set cluster.routing.allocation.enable=%s"
                " via %s", mode, main)
        else:
            logger.info("Set cluster.routing.allocation.enable=%s cluster-wide", mode)

    @property
    def allowed_aliases(self) -> list:
        """Required by SREBatchRunnerBase"""
        return list(CLUSTERGROUPS)

    @property
    def restart_daemons(self) -> list:
        """Property to return a list of daemons to restart"""
        # The service name depends on the cluster
        # datahubsearch: opensearch_1@datahub.service
        # logstash: opensearch_2@production-elk7-eqiad.service
        # As systemd suports glob syntax in its parameter syntax
        # (https://www.freedesktop.org/software/systemd/man/systemctl.html#Parameter%20Syntax),  # noqa: E501
        # we can generalize this into a simple pattern
        daemons = ["opensearch_[1-2]@*.service"]

        # The logging clusters (logstash-eqiad/codfw) colocate logstash
        # and opensearch-dashboards on the same nodes, restart those too
        if (self._args.alias in ("logstash-eqiad", "logstash-codfw")
                and self._current_host_is_collector):
            daemons += ["logstash.service", "opensearch-dashboards.service"]

        return daemons

    @contextmanager
    def _shard_allocation_restricted(self):
        """Restrict shard allocation to primaries and restore to all afterwords"""

        if self._args.without_shard_allocation:
            yield
            return
        self.set_shard_allocation("primaries")
        try:
            yield
        finally:
            self.set_shard_allocation("all")

    def _run_action(self, hosts: RemoteHosts) -> None:
        """Perform the actual reboot/restart action"""

        super().action(hosts)

    def _depool_and_act(self, hosts: RemoteHosts) -> None:
        """Depool/pool via conftool on logstash collector hosts"""

        fqdn = hosts.hosts[0]
        self._current_host_is_collector = self._is_collector(fqdn)
        if not self._current_host_is_collector:
            logger.info("%s is not a logstash collector, "
                        "skipping depool/pool", fqdn)
            self._run_action(hosts)
            return

        try:
            next(self._confctl.get(name=fqdn))
        except ConfctlError:
            # logstash collector, but no conftool entry after all
            logger.warning(
                "%s is a collector but has no conftool entry, "
                "skipping depool/pool", fqdn)
            self._run_action(hosts)
            return

        logger.info("%s is a logstash collector, "
                    "including depool/pool steps", fqdn)
        try:
            with self._confctl.change_and_revert(
                    "pooled", "yes", "no", name=fqdn) as objects:
                if not objects:
                    logger.warning(
                        "%s matched conftool but not with pooled=yes"
                        "depool was a no-op",
                        fqdn)
                else:
                    logger.info("Depooled %s via conftool", fqdn)
                self._sleep(self.depool_sleep)
                self._run_action(hosts)
            if objects:
                logger.info("Repooled %s via conftool", fqdn)
        except ConfctlError as error:
            logger.error(
                "Failed to depool/repool %s via conftool: %s", fqdn, error)
            raise

    def action(self, hosts: RemoteHosts) -> None:
        """Manage shard allocation and act on hosts"""

        with self._shard_allocation_restricted():
            self._depool_and_act(hosts)

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Make sure the cluster is in a green state before proceeding"""

        self.check_for_green_indices()

    def post_action(self, hosts: RemoteHosts) -> None:
        """Wait until the cluster has recovered before proceeding with the next action"""

        self.check_for_green_indices()
