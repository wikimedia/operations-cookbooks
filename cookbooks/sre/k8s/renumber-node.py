"""Change vlan and IP of a node in a Kubernetes cluster"""

import logging

from argparse import ArgumentParser, Namespace
from time import sleep
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.k8s import Kubernetes
from spicerack.remote import RemoteError
from wmflib import phabricator
from wmflib.interactive import ask_confirmation

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES

logger = logging.getLogger(__name__)


class RenumberSingleHost(CookbookBase):
    """Change vlan and IP of a node in a Kubernetes cluster

    For a node in a Kubernetes cluster it will:
    - Cordon the node
    - Drain it
    - Set its pooled status to inactive
    - Reimage it with the new vlan and IP
    - Run homer on the core-router and leaf switch to update BGP
    - Check calicoctl node status
    - Uncordon the node
    - Set its pooled status to yes

    Usage example:
        cookbook sre.k8s.renumber-node wikikube-worker2001.codfw.wmnet
    """

    def get_runner(self, args: Namespace) -> "RenumberSingleHostRunner":
        """As specified by Spicerack API."""
        return RenumberSingleHostRunner(args, self.spicerack)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument("-t", "--task-id", help="An optional task ID to post a message to (i.e. T12345).")
        parser.add_argument(
            "-R",
            "--renamed",
            action="store_true",
            help="Adds appropriate switches to reimage cookbook, sets BGP in netbox",
        )
        parser.add_argument(
            "host", help="A single host to be renumbered (specified in Cumin query syntax)"
        )

        return parser


class RenumberSingleHostRunner(CookbookRunnerBase):
    """Renumber a single host."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Renumber a single host."""
        self.args = args
        self.spicerack = spicerack
        self.phabricator: Optional[phabricator.Phabricator] = None

        if "wikikube-worker" not in args.host:
            raise RuntimeError("Only wikikube-worker nodes can be renumbered")

        # Find the host and its k8s metadata
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
                raise RuntimeError("Only a single server can be renumbered")

        if self.remote_host is None:
            raise RuntimeError(
                f"Cannot find the host {args.host} among any k8s workers alias {ALLOWED_CUMIN_ALIASES.keys()}"
            )

        logger.debug("Found host %s in %s", args.host, k8s_metadata["workers"])

        self.host = self.remote_host.hosts[0]
        self.host_short = self.host.split(".")[0]
        self.k8s_cli = Kubernetes(
            group=k8s_metadata["k8s-group"],
            cluster=k8s_metadata["k8s-cluster"],
            dry_run=spicerack.dry_run,
        )

        self.k8s_node = self.k8s_cli.get_node(self.host)
        logger.debug("Found node %s in %s", self.host, k8s_metadata["workers"])

        # Get switch names from netbox
        self.switches_to_update: list[str] = []
        netbox = self.spicerack.netbox(read_write=True)
        netbox_server = spicerack.netbox_server(self.host_short, read_write=False)
        netbox_data = netbox_server.as_dict()
        self.netbox_host = netbox.api.dcim.devices.get(netbox_data["id"])
        self.switches_to_update.append(f"cr*{self.netbox_host.site.slug}*")
        logger.debug("Switches to update: %s", self.switches_to_update)
        self.switches_to_update.append(
            f"{self.netbox_host.primary_ip4.assigned_object.connected_endpoints[0].device.name}*"
        )
        logger.debug("Switches to update: %s", self.switches_to_update)

        # Administrative setup
        self.actions = self.spicerack.actions
        self.host_actions = self.actions[self.remote_host]
        self.reason = self.spicerack.admin_reason(f"Renumbering {self.host}")

        if args.task_id is not None:
            self.phabricator = self.spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            message = f"Cookbook {__name__} was started by {self.reason.owner} {self.runtime_description}\n"
            self.post_to_phab(message)
        else:
            self.phabricator = None

    @property
    def runtime_description(self) -> str:
        """Return a nicely formatted string that represents the cookbook action."""
        return "{} for host {}".format("Renumbering", self.host)

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.host).split(".", 1)[0], concurrency=1, ttl=600)

    def post_to_phab(self, message: Optional[str] = None) -> None:
        """Comment on the phabricator task"""
        if self.phabricator is not None:
            if message is None:
                message = (
                    f"Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:\n"
                    f"{self.actions}\n"
                )
            self.phabricator.task_comment(self.args.task_id, message)

    def depool(self):
        """Depool the node"""
        action_str = f"Depooling node {self.host}"
        logger.info(action_str)
        cookbook_retcode = self.spicerack.run_cookbook(
            "sre.k8s.pool-depool-node",
            [
                "--reason",
                f"'Triggered by {__name__}: {self.reason.reason}'",
                "depool",
                self.host,
            ],
        )
        if cookbook_retcode == 0:
            action_str = f"Successfully cordoned node {self.host}"
            logger.info(action_str)
            self.host_actions.success(action_str)
        else:
            action_str = f"Failed to cordon node {self.host}"
            self.host_actions.failure(f"**{action_str}**, sre.k8s.pool-depool-node returned {cookbook_retcode}")
            logger.error(action_str)
            raise RuntimeError(action_str)

    def pool(self):
        """Repool the node"""
        action_str = f"Pooling node {self.host}"
        logger.info(action_str)
        cookbook_retcode = self.spicerack.run_cookbook(
            "sre.k8s.pool-depool-node",
            ["--reason", f"'Triggered by {__name__}: {self.reason.reason}'", "pool", self.host],
        )
        if cookbook_retcode == 0:
            action_str = f"Pooled and uncordoned node {self.host}"
            logger.info(action_str)
            self.host_actions.success(action_str)
        else:
            action_str = f"Failed to pool and uncordon node {self.host}"
            self.host_actions.failure(f"**{action_str}**, sre.k8s.pool-depool-node returned {cookbook_retcode}")
            logger.error(action_str)
            raise RuntimeError(action_str)

    def reimage(self):
        """Reimage the node and move vlan"""
        action_str = f"Reimaging node {self.host}"
        logger.info(action_str)
        reimage_args = ["--move-vlan", "--os", "bullseye"]
        if self.args.renamed:
            reimage_args.extend(["--new", "--puppet", "7"])
        if self.phabricator:
            reimage_args.extend(["--task-id", self.task_id])
        reimage_args.append(self.host_short)
        logger.info("Running sre.hosts.reimage %s", " ".join(reimage_args))
        cookbook_retcode = self.spicerack.run_cookbook("sre.hosts.reimage", reimage_args)
        if cookbook_retcode == 0:
            action_str = f"Successfully reimaged node {self.host}"
            logger.info(action_str)
            self.host_actions.success(action_str)
        else:
            action_str = f"Failed to reimage node {self.host}"
            self.host_actions.failure(f"**{action_str}**, sre.hosts.reimage returned {cookbook_retcode}")
            logger.error(action_str)
            raise RuntimeError(action_str)

    def prompt_homer(self):
        """Prompt user to run homer on the core-router and on the leaf switch to update BGP configuration"""
        tor_switches = [switch for switch in self.switches_to_update if "cr" not in switch]
        logger.info("Please run the following homer commands")
        logger.info("Don't forget to !log on #wikimedia-operations")
        logger.info("----------------------------------------------------")
        for switch in self.switches_to_update:
            if switch in tor_switches:
                logger.info("# Before continuing")
            else:
                logger.info("# Long, can be run in parallel while finishing the cookbook")
            logger.info("homer %s commit '%s'", switch, self.task_id)
        logger.info("----------------------------------------------------")

        try:
            ask_confirmation(f"Running homer on {','.join(tor_switches)} is mandatory before pooling, continue?")
        except Exception:
            action_str = "Failed to confirm homer commands"
            self.host_actions.failure(f"**{action_str}**")
            logger.error(action_str)
            raise

    def netbox_commit(self):
        """Change device setting for BGP in Netbox"""
        logger.info("Setting BGP to true in Netbox")
        try:
            self.netbox_host.custom_fields["bgp"] = True
            self.netbox_host.save()
            self.host_actions.success("Successfully set BGP to true in Netbox")
        except Exception:
            action_str = "Failed to commit BGP change to Netbox"
            self.host_actions.failure(f"**{action_str}**")
            logger.error(action_str)
            raise

    def run(self):
        """Perform the renumbering and vlan switch"""
        if self.args.renamed:
            logger.info("Skip depooling for renamed hosts, it should have been done already on the old name")
        else:
            try:
                self.depool()
            except Exception:
                logger.info("%s failed:\n%s\n", __name__, self.actions)
                self.post_to_phab()
                raise

        try:
            self.reimage()
        except Exception:
            logger.info("%s failed:\n%s\n", __name__, self.actions)
            self.post_to_phab()
            raise

        if self.args.renamed:
            try:
                self.netbox_commit()
            except Exception:
                logger.info("%s failed:\n%s\n", __name__, self.actions)
                self.post_to_phab()
                raise

        try:
            self.prompt_homer()
        except Exception:
            logger.info("%s failed:\n%s\n", __name__, self.actions)
            self.post_to_phab()
            raise

        logger.info("Sleep 60s before pooling to allow BGP to Establish")
        sleep(60)
        try:
            self.pool()
        except Exception:
            logger.info("%s failed:\n%s\n", __name__, self.actions)
            self.post_to_phab()
            raise

        logger.info("%s completed:\n%s\n", __name__, self.actions)
        self.post_to_phab()
        if self.host_actions.has_failures:
            return 1

        return 0
