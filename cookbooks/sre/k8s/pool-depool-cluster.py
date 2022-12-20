"""Pool/Depool all services running in a Kubernetes cluster"""
import logging
from argparse import ArgumentParser, Namespace
from typing import List

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.service import Service
from wmflib.constants import CORE_DATACENTERS
from wmflib.interactive import ask_confirmation

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
        parser.add_argument('--reason', required=False, help='Admin reason')
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

        if len(self.services) == 1:
            services_msg = self.services[0]
        else:
            services_msg = f'{len(self.services)} services'

        reason = self.args.reason if self.args.reason else "maintenance"
        return f"{self.args.action} {services_msg} in {cluster_str}: {reason}"

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
                # The 'kubesvc' conftool settings are shared across
                # multiple services, and they basically represent
                # if a k8s node gets traffic from LVS or not.
                # In this cookbook we are interested in the DNS discovery
                # settings, so we don't touch the kubesvc config at all.
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
        # We make an exception for active/passive services, since it is not
        # always safe to perform automatic actions on them.
        # Use cases:
        # 1)   If a service does not have a discovery record set in Puppet's
        #      service catalog, we can safely skip it.
        # 2-1) If the action is 'check', we can safely proceed.
        # 2-2) If the action is pool/depool and the service is listed as
        #      active/active, then we have an indication that it should be safe
        #      to proceed. The depool action in spicerack will check if the DC
        #      is depoolable or not, so no extra safe check needed.
        # 4)   If the action is pool/depool and the service is active/passive,
        #      we shouldn't take any action automatically but just warn the operator.
        #      We don't want to cause an outage due to operator mistakes.
        discovery_names = set()
        for service in self.services:
            if not service.discovery:
                logger.info("The service %s does not have a discovery "
                            "configuration in the Puppet service catalog, skipping.",
                            service.name)
            elif self.args.action == "check" or service.discovery.active_active:
                discovery_names.update([d.dnsdisc for d in service.discovery])
            else:
                other_site = service.discovery.site if service.discovery.site == "eqiad" else "codfw"

                if self.args.action == "pool":
                    confctl_commands = (
                        "confctl --object-type discovery select "
                        f"'dnsdisc={service.discovery.name},name={service.discovery.site}' set/pooled=true \n"
                        "confctl --object-type discovery select "
                        f"'dnsdisc={service.discovery.name},name={other_site}' set/pooled=false \n"
                    )
                else:
                    confctl_commands = (
                        "confctl --object-type discovery select "
                        f"'dnsdisc={service.discovery.name},name={other_site}' set/pooled=true \n"
                        "confctl --object-type discovery select "
                        f"'dnsdisc={service.discovery.name},name={service.discovery.site}' set/pooled=false \n"
                    )

                logger.info(
                    "The service %s is not listed as active/active "
                    "in the Puppet service catalog. "
                    "The cookbook requires a manual intervention from the operator "
                    "to avoid any unwanted side effects. \n"
                    "Execute the following commands on the puppetmaster. "
                    "Verify the current status of the discovery record, namely "
                    "which datacenter is pooled and which one is depooled. "
                    "confctl --quiet --object-type discovery select 'dnsdisc=%s' get \n"
                    "Following the cookbook's arguments, these are commands to execute "
                    "(please make sure that they are consistent with the status outlined above "
                    "before proceeding!): "
                    "\n%s\n"
                    "NOTE: please keep in mind that each DNS record has a TTL value, "
                    "so any change will be reflected after the cache expires.",
                    service.name, service.name, confctl_commands
                )
                ask_confirmation("Please confirm to have read the above before proceeding.")

        run_args = [self.args.action]
        if self.args.action in ("pool", "depool"):
            run_args.append(self.args.datacenter)
            if self.args.wipe_cache:
                run_args.append("--wipe-cache")
        run_args.extend(discovery_names)

        return self.spicerack.run_cookbook("sre.discovery.service-route", run_args)
