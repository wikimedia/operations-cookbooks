"""Pool/Depool all services running in a Kubernetes cluster"""
import logging
from argparse import ArgumentParser, Namespace
from typing import List

from spicerack import Spicerack
from spicerack.constants import CORE_DATACENTERS
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.service import Service

logger = logging.getLogger(__name__)


class PoolDepoolCluster(CookbookBase):
    """Pool/Depool all services running in a Kubernetes cluster.

    This cookbooks looks up services of a Kubernetes cluster via the service catalog and pools/depools them via dnsdisc.

    Usage example:
    To check the state of all services in the codfw wikikube cluster:
        cookbook.sre.k8s.pool-depool-cluster check codfw
    Depool all services from wikikube codfw (looks confusing because cluster and DC are named the same):
        cookbook.sre.k8s.pool-depool-cluster depool --wipe-cache codfw codfw
    """

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments for all the sre.discovery cookbooks."""
        parser = super().argument_parser()
        actions = parser.add_subparsers(dest="action", help="The action to perform")
        action_check = actions.add_parser("check")
        action_pool = actions.add_parser("pool")
        action_depool = actions.add_parser("depool")

        for action in (action_check, action_pool, action_depool):
            if action is not action_check:
                action.add_argument(
                    "datacenter",
                    choices=CORE_DATACENTERS,
                    help="Name of the datacenter. One of: %(choices)s.",
                )
                action.add_argument(
                    "--wipe-cache",
                    action="store_true",
                    help="Wipe the cache on DNS recursors.",
                )
            action.add_argument(
                "cluster",
                help="Name (as in hiera kubernetes_cluster_groups) of the cluster",
            )

        return parser

    def get_runner(self, args: Namespace) -> "PoolDepoolClusterRunner":
        """As specified by Spicerack API."""
        return PoolDepoolClusterRunner(args, self.spicerack)


class PoolDepoolClusterRunner(CookbookRunnerBase):
    """Pool/Depool cookbook runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        self.args = args
        self.spicerack = spicerack
        self.services = self.get_services()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        if self.args.action != "check":
            cluster_str = f"{self.args.datacenter}/{self.args.cluster}"
        else:
            cluster_str = self.args.cluster
        return f"{self.args.action} {len(self.services)} in {cluster_str}: {self.admin_reason.reason}"

    def get_services(self) -> List[Service]:
        """Return a list of all services running in a specific Kubernetes cluster"""
        services = []

        # Cluster names in hiera (kubernetes_cluster_groups) and service.yaml do not match
        if self.args.cluster.startswith("ml-serve"):
            conftool_cluster = "ml_serve"
        elif self.args.cluster.startswith("ml-staging"):
            conftool_cluster = "ml_staging"
        elif self.args.cluster.startswith("staging"):
            conftool_cluster = "kubernetes-staging"
        elif self.args.cluster in ("codfw", "eqiad"):
            conftool_cluster = "kubernetes"

        for service in self.spicerack.service_catalog():
            try:
                if not service.lvs.enabled:
                    continue
                if service.lvs.conftool.service != "kubesvc":
                    continue
            except AttributeError:
                continue

            if service.lvs.conftool.cluster == conftool_cluster:
                services.append(service)

        return services

    def run(self) -> int:
        """Required by Spicerack API."""
        logger.info(
            "Found %s services for cluster %s", len(self.services), self.args.cluster
        )
        if not self.services:
            return 0

        # Get all discovery names (unique)
        # FIXME: Should we make exceptions for active/passive services?
        discovery_names = set()
        for service in self.services:
            discovery_names.update([d.dnsdisc for d in service.discovery])

        run_args = [self.args.action]
        if self.args.action in ("pool", "depool"):
            run_args.append(self.args.datacenter)
            if self.args.wipe_cache:
                run_args.append("--wipe-cache")
        run_args.extend(discovery_names)

        return self.spicerack.run_cookbook("sre.discovery.service-route", run_args)
