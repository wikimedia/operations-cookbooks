"""Change vlan and IP of a node in a Kubernetes cluster"""

import logging
from argparse import ArgumentParser, Namespace
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.k8s import Kubernetes
from spicerack.remote import RemoteError
from wmflib import phabricator
from wmflib.interactive import ask_confirmation, confirm_on_failure

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import OS_VERSIONS
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
        parser.add_argument("--os", choices=OS_VERSIONS, default="bookworm",
                            help="the Debian version to install. Mandatory parameter. One of %(choices)s.")
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

        if "wikikube-worker" not in args.host and "kubestage" not in args.host:
            raise RuntimeError("Only wikikube-worker/kubestage nodes can be renumbered")

        self.remote_host = None
        self.host = None

        if len(self.args.host.split(".", 1)) < 2 or self.args.host.split(".", 1)[1] not in (
            "eqiad.wmnet",
            "codfw.wmnet",
            "eqiad.wmnet.",
            "codfw.wmnet.",
        ):
            raise RuntimeError(f"Invalid FQDN {self.args.host}")

        if args.renamed:
            self.setup_direct_backend_host()
        else:
            self.setup_k8s_remote_host()

        # Get switch names from netbox
        self.switches_to_update: list[str] = []
        netbox = self.spicerack.netbox(read_write=True)
        netbox_server = self.spicerack.netbox_server(self.host_short, read_write=False)
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

    def check_remote_host(self) -> None:
        """Check if the host exists, and is unique"""
        if self.remote_host is None:
            if self.args.renamed:
                raise RuntimeError(f"Cannot find the host {self.args.host} in direct backend")
            raise RuntimeError(
                (
                    f"Cannot find the host {self.args.host} among any k8s workers alias "
                    f"{','.join(ALLOWED_CUMIN_ALIASES.keys())}, and rename has not been specified"
                )
            )
        if len(self.remote_host) > 1:
            raise RuntimeError(f"Found multiple hosts for {self.args.host} in backend")

    def setup_direct_backend_host(self) -> None:
        """Set up host with direct backend for rename"""
        # The host won't exist in puppet yet, so use the Direct backend
        self.remote_host = self.spicerack.remote().query(f"D{{{self.args.host}}}")
        self.check_remote_host()
        self.host = self.remote_host.hosts[0]
        self.host_short = self.host.split(".")[0]

    def setup_k8s_remote_host(self):
        """Pull Kubernetes metadata information"""
        # Find the host and its k8s metadata
        for _, metadata in ALLOWED_CUMIN_ALIASES.items():
            logger.debug("Checking for host %s in %s", self.args.host, metadata["workers"])
            try:
                self.remote_host = self.spicerack.remote().query(f"P{{{self.args.host}}} and A:{metadata['workers']}")
            except RemoteError:
                continue

            self.check_remote_host()
            k8s_metadata = metadata
            self.k8s_cli = Kubernetes(
                group=k8s_metadata["k8s-group"],
                cluster=k8s_metadata["k8s-cluster"],
                dry_run=self.spicerack.dry_run,
            )
            # Set up host only when called early for a no-rename run
            if self.host is None:
                self.host = self.remote_host.hosts[0]
                self.host_short = self.host.split(".")[0]
            self.k8s_node = self.k8s_cli.get_node(self.host)
            logger.debug("Found node %s in %s", self.host, k8s_metadata["workers"])

            break

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
                str(self.host),
            ],
        )
        if cookbook_retcode == 0:
            self.host_actions.success(f"Successfully cordoned node {self.host}")
        else:
            self.host_actions.failure(
                f"**Failed to cordon node {self.host}**, sre.k8s.pool-depool-node returned {cookbook_retcode}"
            )
            raise RuntimeError(f"sre.k8s.pool-depool-node returned {cookbook_retcode}")

    def pool(self):
        """Repool the node"""
        action_str = f"Pooling node {self.host}"
        logger.info(action_str)
        cookbook_retcode = self.spicerack.run_cookbook(
            "sre.k8s.pool-depool-node",
            ["--reason", f"'Triggered by {__name__}: {self.reason.reason}'", "pool", str(self.host)],
        )
        if cookbook_retcode == 0:
            self.host_actions.success(f"Pooled and uncordoned node {self.host}")
        else:
            self.host_actions.failure(
                f"**Failed to pool and uncordon node {self.host}**, "
                f"sre.k8s.pool-depool-node returned {cookbook_retcode}"
            )
            raise RuntimeError(f"Failed to pool and uncordon node {self.host}")

    def reimage(self):
        """Reimage the node and move vlan"""
        reimage_args = ["--move-vlan", "--os", self.args.os]
        if self.args.renamed:
            reimage_args.extend(["--new", "--puppet", "7"])
        if self.phabricator:
            reimage_args.extend(["--task-id", self.task_id])
        reimage_args.append(self.host_short)
        logger.info("Running sre.hosts.reimage %s", " ".join(reimage_args))
        cookbook_retcode = self.spicerack.run_cookbook("sre.hosts.reimage", reimage_args)
        if cookbook_retcode == 0:
            self.host_actions.success(f"Successfully reimaged node {self.host}")
        else:
            self.host_actions.failure(
                f"**Failed to reimage node {self.host}**, sre.hosts.reimage returned {cookbook_retcode}"
            )
            raise RuntimeError(f"Failed to reimage node {self.host}")

    def prompt_homer(self):
        """Prompt user to run homer to update BGP configuration"""
        tor_switches = [switch for switch in self.switches_to_update if "cr" not in switch]
        logger.info("Please run the following homer commands")
        logger.info("----------------------------------------------------")
        for switch in self.switches_to_update:
            if switch in tor_switches:
                logger.info("# Before continuing")
            else:
                logger.info("# Long, can be run in parallel while finishing the cookbook")
            logger.info("!log homer %s commit '%s'", switch, self.task_id)
            logger.info("homer %s commit '%s'", switch, self.task_id)
        logger.info("----------------------------------------------------")

        try:
            ask_confirmation(f"Running homer on {','.join(tor_switches)} is mandatory before pooling, continue?")
        except Exception:
            self.host_actions.failure("**Failed to confirm homer commands**")
            raise

    def netbox_commit(self):
        """Change device setting for BGP in Netbox"""
        logger.info("Setting BGP to true in Netbox")
        try:
            self.netbox_host.custom_fields["bgp"] = True
            self.netbox_host.save()
            self.host_actions.success("Successfully set BGP to true in Netbox")
        except Exception:
            self.host_actions.failure("**Failed to commit BGP change to Netbox**")
            raise

    def run_puppet_agent_deploy(self):
        """Run puppet agent on the deployment servers"""
        logger.info("Running puppet agent on A:deployment-servers")
        deploy_hosts = self.spicerack.remote().query("A:deployment-servers")
        try:
            self.spicerack.puppet(deploy_hosts).run()
            self.host_actions.success("Successfully ran puppet agent on deployment servers")
        except Exception:
            self.host_actions.failure("**Failed to run puppet agent on deployment servers**")
            raise

    def run_puppet_agent_registry(self):
        """Run puppet agent on the registry servers"""
        logger.info("Running puppet agent on A:docker-registry")
        registry_hosts = self.spicerack.remote().query("A:docker-registry")
        try:
            self.spicerack.puppet(registry_hosts).run()
            self.host_actions.success("Successfully ran puppet agent on registry servers")
        except Exception:
            self.host_actions.failure("**Failed to run puppet agent on registry servers**")
            raise

    def rollback(self) -> None:
        """Does nothing but log and post to phabricator on error as rollback isn't supported"""
        logger.error("%s failed:\n%s\n", __name__, self.actions)
        self.post_to_phab()

    def run(self):
        """Perform the renumbering and vlan switch"""
        if self.args.renamed:
            logger.info("Skip depooling for renamed hosts, it should have been done already on the old name")
        else:
            self.depool()

        self.reimage()

        if self.args.renamed:
            self.netbox_commit()
            self.setup_k8s_remote_host()

        self.prompt_homer()
        confirm_on_failure(self.run_puppet_agent_deploy)
        confirm_on_failure(self.run_puppet_agent_registry)
        self.pool()
        logger.info("%s completed:\n%s\n", __name__, self.actions)
        self.post_to_phab()
        if self.host_actions.has_failures:
            return 1

        return 0
