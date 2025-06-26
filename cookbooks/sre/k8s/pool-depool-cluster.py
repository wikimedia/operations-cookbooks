"""Pool/Depool all services running in a Kubernetes cluster"""

import logging
from argparse import ArgumentParser, Namespace

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase
from wmflib.constants import CORE_DATACENTERS

from cookbooks.sre.discovery.datacenter import (
    DiscoveryDcRoute,
    DiscoveryDcRouteRunner,
    DiscoveryRecord,
)
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES, conftool_cluster_name

logger = logging.getLogger(__name__)


class PoolDepoolCluster(DiscoveryDcRoute, CookbookBase):
    """Pool/Depool all services running in a Kubernetes cluster.

    This cookbooks looks up services of a Kubernetes cluster via the service catalog and pools/depools them via dnsdisc.

    Usage example:
    To check the state of all services in the codfw wikikube cluster:
        cookbook.sre.k8s.pool-depool-cluster status --k8s-cluster wikikube-codfw
    Depool all services from wikikube staging codfw cluster:
        cookbook.sre.k8s.pool-depool-cluster depool --k8s-cluster staging-codfw
    """

    argument_reason_required = False
    _actions = ("pool", "depool", "status")

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments for all the sre.discovery cookbooks."""
        parser = CookbookBase.argument_parser(self)
        actions = parser.add_subparsers(dest="action", help="The action to perform")

        for act in self._actions:
            action = actions.add_parser(act)
            if act in ("pool", "depool"):
                action.add_argument(
                    "-r",
                    "--reason",
                    required=False,
                    help="Admin reason",
                    default="maintenance",
                )
                action.add_argument(
                    "--all",
                    action="store_true",
                    help="Act on the active/passive services (minus MediaWiki) as well",
                )
            action.add_argument(
                "--k8s-cluster",
                required=True,
                help="K8s cluster to use for downtimes, sanity checks and Cumin aliases",
                choices=ALLOWED_CUMIN_ALIASES.keys(),
            )

        return parser

    def get_runner(self, args: Namespace) -> "PoolDepoolClusterRunner":
        """As specified by Spicerack API."""
        if not args.action:
            raise RuntimeError(
                f"You must specify an action ({', '.join(self._actions)})"
            )
        # Default filter to false, since we don't have the argument specified in this cookbook.
        # The argument_postprocess will set it to True for anything by the status action.
        args.filter = False
        if args.action == "status":
            args.datacenter = "all"
        else:
            # Get the datacenter from the cluster name and store it as datacenter argument.
            args.datacenter = args.k8s_cluster.split("-")[-1]
            if args.datacenter not in CORE_DATACENTERS:
                raise RuntimeError(
                    f"Invalid datacenter part in k8s-cluster argument ({args.datacenter})."
                    f"Must be one of: {', '.join(CORE_DATACENTERS)}"
                )
        self.argument_postprocess(args)
        args.fast_insecure = False  # Insecure mode is not supported by this cookbook
        return PoolDepoolClusterRunner(args, self.spicerack)


class PoolDepoolClusterRunner(DiscoveryDcRouteRunner):
    """Pool/Depool cookbook runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        self.args = args
        self.conftool_cluster = conftool_cluster_name(args.k8s_cluster)
        super().__init__(args, spicerack)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        if self.args.action != "status":
            cluster_str = f"{self.args.datacenter}/{self.args.k8s_cluster}"
        else:
            cluster_str = self.args.k8s_cluster

        reason = self.args.reason if self.args.reason else "maintenance"
        return f"{self.args.action} all services in {cluster_str}: {reason}"

    def _get_all_services(self) -> dict[str, list[DiscoveryRecord]]:
        all_services: dict[str, list[DiscoveryRecord]] = {
            "active_active": [],
            "active_passive": [],
        }

        # We exclude (in addition to what is filtered by the superclass):
        # - services that are not LVS enabled
        # - services that are not kubesvc
        for mode, records in super()._get_all_services().items():
            for record in records:
                service = self.catalog.get(record.service_name)
                if service.lvs is None or not service.lvs.enabled:
                    logger.debug("Skipping %s, as it is not LVS enabled", service.name)
                    continue
                if service.lvs.conftool.service != "kubesvc":
                    logger.debug(
                        "Skipping %s, as it is not running on kubernetes", service.name
                    )
                    continue
                if service.lvs.conftool.cluster != self.conftool_cluster:
                    logger.debug(
                        "Skipping %s, as it is not on the %s cluster",
                        service.name,
                        self.conftool_cluster,
                    )
                    continue
                all_services[mode].append(record)

        logger.info(
            "Found %d active/active and %d active/passive services on %s/%s",
            len(all_services["active_active"]),
            len(all_services["active_passive"]),
            self.args.datacenter,
            self.args.k8s_cluster,
        )
        return all_services
