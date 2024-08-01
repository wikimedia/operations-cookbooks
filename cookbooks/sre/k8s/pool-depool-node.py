"""Change pooled status of a node in a Kubernetes cluster"""

import logging

from argparse import ArgumentParser, Namespace
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.k8s import Kubernetes
from spicerack.remote import RemoteError, RemoteHosts
from wmflib import phabricator

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES

logger = logging.getLogger(__name__)


class PoolDepoolSingleHost(CookbookBase):
    """Change pooled status of a node in a Kubernetes cluster

    For a node in a Kubernetes cluster it will:
    - Cordon the node
    - Drain it
    - Set its pooled status to inactive
    Or:
    - Uncordon the node
    - Set its pooled status to yes

    Usage example:
        cookbook sre.k8s.pool-depool-node pool wikikube-worker2001.codfw.wmnet
        cookbook sre.k8s.pool-depool-node depool wikikube-worker2001.codfw.wmnet
    """

    def get_runner(self, args: Namespace) -> "PoolDepoolSingleHostRunner":
        """As specified by Spicerack API."""
        return PoolDepoolSingleHostRunner(args, self.spicerack)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "-r",
            "--reason",
            required=False,
            help=(
                "The reason for the pooling/depooling. The current username and originating host are "
                "automatically added."
            ),
        )
        parser.add_argument("-t", "--task-id", help="An optional task ID to post a message to (i.e. T12345).")
        actions = parser.add_subparsers(dest="action", help="The action to perform")
        action_pool = actions.add_parser("pool")
        action_depool = actions.add_parser("depool")
        action_check = actions.add_parser("check")
        for action in (action_pool, action_depool, action_check):
            action.add_argument(
                "host", help="A single host to be pooled/depooled/checked (specified in Cumin query syntax)"
            )

        return parser


class PoolDepoolSingleHostRunner(CookbookRunnerBase):
    """Drain and depool a single host."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Drain a single host and depool it"""
        self.args = args
        self.spicerack = spicerack
        self.phabricator: Optional[phabricator.Phabricator] = None
        self.remote_host: Optional[RemoteHosts] = None

        for _, metadata in ALLOWED_CUMIN_ALIASES.items():
            logger.debug("Checking for host %s in %s", args.host, metadata["workers"])
            try:
                self.remote_host = spicerack.remote().query(f"P{{{args.host}}} and A:{metadata['workers']}")
            except RemoteError:
                continue

            if len(self.remote_host) == 1:
                k8s_metadata = metadata
                break
            if len(self.remote_host) > 1:
                raise RuntimeError("Only a single server can be pooled or depooled")

        if self.remote_host is None:
            raise RuntimeError(
                f"Cannot find the host {args.host} among any k8s workers alias " f"{ALLOWED_CUMIN_ALIASES.keys()}"
            )

        logger.debug("Found host %s in %s", args.host, k8s_metadata["workers"])
        self.host = args.host
        self.k8s_workers = k8s_metadata["workers"]
        self.k8s_cli = Kubernetes(
            group=k8s_metadata["k8s-group"],
            cluster=k8s_metadata["k8s-cluster"],
            dry_run=spicerack.dry_run,
        )

        self.k8s_node = self.k8s_cli.get_node(self.host)
        logger.debug("Found node %s in %s", self.host, self.k8s_workers)

        self.confctl = self.spicerack.confctl("node")

        self.reason = self.spicerack.admin_reason(f"{args.action} {self.host}" if not args.reason else args.reason)

        if args.task_id is not None:
            self.phabricator = phabricator.create_phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            self.message = f"{args.action} host {self.host} by {self.reason.owner} with reason: {args.reason}\n"
        else:
            self.phabricator = None

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return "{} for host {}".format(self.args.action, self.host)

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.host).split(".", 1)[0], concurrency=1, ttl=600)

    def run(self):
        """Uncordon and pool or cordon, drain, and depool the host"""
        logger.debug("Looking for confctl objects for host %s", self.host)
        confctl_services = self.confctl.filter_objects({}, name=self.host, service="kubesvc")
        if not confctl_services:
            raise RuntimeError(f"No kubesvc confctl objects found for host {self.host}")

        if self.args.action == "pool":
            logger.info("Pooling %s in %s", self.host, self.k8s_workers)
            self.confctl.update_objects({"pooled": "yes", "weight": 10}, confctl_services)
            self.k8s_node.uncordon()
        elif self.args.action == "depool":
            self.k8s_node.cordon()
            logger.info("Draining %s", self.host)
            self.k8s_node.drain()
            logger.info("Depooling %s from %s", self.host, self.k8s_workers)
            self.confctl.update_objects({"pooled": "inactive"}, confctl_services)
        elif self.args.action == "check":
            for service in confctl_services:
                logger.info(
                    "%s confctl status: %s",
                    service.name,
                    "pooled" if getattr(service, "pooled") == "yes" else getattr(service, "pooled"),
                )

            logger.info(
                "%s kubernetes status in %s: %s",
                self.host,
                self.k8s_workers,
                "schedulable" if self.k8s_node.is_schedulable() else "unschedulable",
            )
