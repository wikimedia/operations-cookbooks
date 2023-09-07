"""Perform rolling operations on opensearch servers"""

import logging
from argparse import Namespace
from datetime import timedelta

from requests.exceptions import RequestException
from spicerack import Spicerack
from spicerack.decorators import retry
from spicerack.remote import RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)

CLUSTERGROUPS = ("datasearchub",)


class ClusterHealthNotGreen(Exception):
    """Exception raised when an OpenSearch cluster health status is *not* green"""


class RollingOperation(SREBatchBase):
    """Perform a rolling operation on servers of an opensearch cluster.

    The command can either restart the opensearch daemons or reboot the hosts.

    Usage examples:
        cookbook sre.opensearch.rolling-restart-reboot \
            --alias datahubsearch \
            --reason "Rolling reboot to pick up new kernel" \
            reboot

        cookbook sre.opensearch.rolling-restart-reboot \
            --alias datahubsearch \
            --reason "Rolling restart to pick new OpenSSL" \
            restart_daemons
    """

    grace_sleep = 120
    min_grace_sleep = 60

    # We must implement this abstract method
    def get_runner(self, args: Namespace):
        """As specified by Spicerack API."""
        return RollingOperationRunner(args, self.spicerack)


class RollingOperationRunner(SREBatchRunnerBase):
    """Apply rolling operation to cluster."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Required by SREBatchRunnerBase"""
        super().__init__(args, spicerack)
        self.http_session = self._spicerack.requests_session(__name__)

    @retry(
        tries=60,
        delay=timedelta(seconds=10),
        backoff_mode="constant",
        exceptions=(ClusterHealthNotGreen, RequestException),
    )
    def check_for_green_indices(self, hosts: RemoteHosts):
        """Make sure the cluster indices are all green"""
        for host in hosts.hosts:
            resp = self.http_session.get(f"http://{host}:9200/_cluster/health")
            resp.raise_for_status()
            data = resp.json()
            if data["status"] != "green":
                raise ClusterHealthNotGreen(
                    f"[{host}] Cluster is in status {data['status']}"
                )
        logger.info("Cluster health is green")

    @property
    def allowed_aliases(self) -> list:
        """Required by SREBatchRunnerBase"""
        return list(CLUSTERGROUPS)

    @property
    def restart_daemons(self) -> list:
        """Property to return a list of daemons to restart"""
        return ["opensearch_1@datahub"]

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Make sure the cluster is in a green state before proceeding with the action"""
        self.check_for_green_indices(hosts)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Wait until the cluster has recovered until proceeding with the next action"""
        self.check_for_green_indices(hosts)
