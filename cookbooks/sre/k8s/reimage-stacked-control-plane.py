"""Reimage stacked kubernetes control planes (one by one) without loosing etcd data"""

import json
import logging
import time
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import Optional

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.interactive import (
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
)
from wmflib.phabricator import Phabricator

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.hosts import OS_VERSIONS
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES, etcdctl, etcd_cluster_healthy

logger = logging.getLogger(__name__)


class ReimageControlPlanes(CookbookBase):
    """Reimage stacked kubernetes control planes (one by one) without loosing etcd data.

    Workflow:
    - Ensure etcd cluster is healthy and not in boostrap mode (profile::etcd::v3::cluster_bootstrap: false)
    - Downtime control-plane
    - Depool
    - Disable puppet and stop etcd
      This is to be able to stop etcd so we can safely remove and re-add the etcd member before
      starting the reimage.
    - Delete control plane from etcd cluster (member remove)
    - Re-add control plane to etcd cluster
    - Reimage
    - Sanity check
    - Repool
    - Remove downtime
    """

    argument_reason_required = True
    argument_task_required = False

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--k8s-cluster",
            required=True,
            help="K8s cluster to use for downtimes, sanity checks and Cumin aliases",
            choices=ALLOWED_CUMIN_ALIASES.keys(),
        )
        parser.add_argument(
            "--control-plane-query",
            required=False,
            help=(
                "Optional Cumin query to use a subset of the control plane nodes "
                "(You need to use the Cumin global grammar, see "
                "https://wikitech.wikimedia.org/wiki/Cumin#Global_grammar_host_selection)"
            ),
        )
        parser.add_argument(
            "--os",
            required=True,
            help="Debian OS codename to use for the reimages",
            choices=OS_VERSIONS,
        )
        parser.add_argument(
            '--use-http-for-dhcp', action='store_true', default=False,
            help=(
                "Fetching the DHCP config via HTTP is quicker, "
                "but we've run into issues with various NIC firmwares "
                "when operating in BIOS mode. As such we default to the slower, "
                "yet more reliable TFTP for BIOS. If a server is known "
                "to be working fine with HTTP, it can be forced with this option."
            )
        )
        return parser

    def get_runner(self, args: Namespace) -> "ReimageControlPlanesRunner":
        """As specified by Spicerack API."""
        return ReimageControlPlanesRunner(args, self.spicerack)


class ReimageControlPlanesRunner(CookbookRunnerBase):
    """Reimage kubernetes control planes (one by one) without loosing etcd data, runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self.args = args
        self.k8s_cluster = args.k8s_cluster
        self.spicerack = spicerack
        self.reason = spicerack.admin_reason(args.reason)
        self.spicerack_remote = spicerack.remote()
        self.confctl = self.spicerack.confctl("node")
        if self.args.task_id is not None:
            self.phabricator: Optional[Phabricator] = spicerack.phabricator(
                PHABRICATOR_BOT_CONFIG_FILE
            )
        else:
            self.phabricator = None

        # Query all control plane nodes (required to run etcd commands on) in this k8s cluster
        # and optionally filter the nodes to work on by the provided query.
        self.all_control_plane_nodes: RemoteHosts = self.spicerack_remote.query(
            self.control_plane_query
        )
        if self.args.control_plane_query:
            self.nodes: RemoteHosts = self.spicerack_remote.query(
                f"{self.control_plane_query} and {self.args.control_plane_query}"
            )
        else:
            self.nodes = self.all_control_plane_nodes

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"Reimaging k8s control planes of cluster {self.k8s_cluster}: {self.args.reason}"

    @property
    def control_plane_query(self):
        """Returns a safe Cumin query to use for stacked control plane nodes"""
        query = f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['control-plane']} and A:all-etcd"
        return query

    @property
    def lock_args(self) -> LockArgs:
        """Make the cookbook lock per k8s cluster"""
        return LockArgs(
            suffix=self.k8s_cluster, concurrency=1, ttl=len(self.nodes) * 3600
        )

    def _get_any_other_remote(self, current_host: str) -> RemoteHosts:
        """Return a RemoteHost instance of a control plane node that is not the one being re-imaged"""
        for host in self.all_control_plane_nodes.hosts:
            if host != current_host:
                return self.spicerack_remote.query(host)
        raise RuntimeError("No other control plane node found")

    def rollback(self):
        """Update the Phabricator task with the actions already taken."""
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                (
                    f"Cookbook {__name__} started by {self.reason.owner} "
                    f"{self.runtime_description} executed with errors:"
                    f"\n{self.spicerack.actions}\n"
                ),
            )

    def run(self) -> None:
        """Required by Spicerack API."""
        # Check if the etcd cluster is in bootstrap mode
        try:
            self.all_control_plane_nodes.run_sync(
                "/usr/bin/grep -q 'ETCD_INITIAL_CLUSTER_STATE=\"existing\"' /etc/default/etcd",
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
        except RemoteExecutionError as exc:
            raise RuntimeError(
                "Etcd cluster is in bootstrap mode, please disable it before proceeding "
                "by setting the hiera key 'profile::etcd::v3::cluster_bootstrap' to false"
            ) from exc

        for host in self.nodes.hosts:
            remote = self.spicerack_remote.query(host)
            any_other_remote = self._get_any_other_remote(host)
            # Sanity check
            if not etcd_cluster_healthy(remote):
                ask_confirmation(
                    "etcd cluster is in an unhealthy state. "
                    "Do you want to continue anyway?"
                )

            # Fetch the member_id of the control plane node to be re-imaged
            result = remote.run_sync(
                etcdctl("-w json endpoint status"),
                is_safe=True,
                print_progress_bars=False,
                print_output=False,
            )
            _, output = next(result)
            # Stdout and stderr are merged in the output but etcdctl always prints JSON
            # before everything else, so we can just parse the first line.
            status = json.loads(next(output.lines()))
            try:
                # etcdctl json output gives member/leader IDs in decimal format
                # but all commands expect them in hex
                member_id = f"{status[0]['Status']['header']['member_id']:x}"
                leader_id = f"{status[0]['Status']['leader']:x}"
                logger.info(
                    "%s has member_id: %s, leader is: %s", host, member_id, leader_id
                )
            except Exception as exc:
                raise RuntimeError(
                    f"Failed to find member_id for {host}. Please check the etcd cluster state"
                ) from exc

            alerting_hosts = self.spicerack.alerting_hosts([host])
            icinga_hosts = self.spicerack.icinga_hosts([host])
            # Downtime and depool
            with alerting_hosts.downtimed(
                self.reason, duration=timedelta(minutes=120)
            ), self.confctl.change_and_revert(
                "pooled",
                "yes",
                "inactive",
                name=host,
            ):
                # Get the management password early so cookbook can continue unattended.
                # The password will be stored in the spicerack instance which will be passed down
                # to the reimage cookbook.
                _ = self.spicerack.management_password()
                if not self.spicerack.dry_run:
                    logger.info(
                        "Waiting for 3 minutes to allow for any in-flight connection to complete"
                    )
                    time.sleep(180)
                # Disable puppet
                puppet = self.spicerack.puppet(remote)
                puppet.disable(self.reason)
                # Stop etcd
                remote.run_sync(
                    "/usr/bin/systemctl stop etcd",
                    print_progress_bars=False,
                    print_output=False,
                )
                # Delete and re-add (as new) the control plane
                any_other_remote.run_sync(
                    etcdctl(f"member remove {member_id}"),
                    etcdctl(f"member add {host} --peer-urls=https://{host}:2380"),
                    print_progress_bars=False,
                )
                # Reimage
                # The reimage cookbook does not work with dry-run
                if not self.spicerack.dry_run:
                    # The reimage cookbook will store it's actions in spicerack.actions
                    # with consecutive calls appending their actions. In order to avoid
                    # posting all the actions after each reimage, we don't pass the task-id
                    # to the reimage cookbook and post the actions once, after all reimages
                    # have been completed.
                    reimage_args = [
                        "--force",
                        "--no-downtime",
                        "--os",
                        self.args.os,
                        host.split(".")[0],
                    ]
                    if self.args.use_http_for_dhcp:
                        reimage_args.insert(0, "--use-http-for-dhcp")
                    return_code = self.spicerack.run_cookbook(
                        "sre.hosts.reimage",
                        reimage_args,
                    )
                    if return_code:
                        ask_confirmation(
                            "The reimage cookbook returned a non-zero code, something "
                            "failed and you'd need to check. Do you want to "
                            "continue anyway?"
                        )
                icinga_hosts.wait_for_optimal(skip_acked=True)
                # Sanity check
                if not etcd_cluster_healthy(any_other_remote):
                    ask_confirmation(
                        "etcd cluster is in an unhealthy state. "
                        "Do you want to continue anyway?"
                    )
                # cluster-info will exit with 1 if the control plane is not up
                remote = self.spicerack_remote.query(host)
                confirm_on_failure(
                    remote.run_sync,
                    "/usr/bin/kubectl cluster-info",
                    is_safe=True,
                    print_progress_bars=False,
                )

        # Post to phab when all reimages have been completed
        if self.phabricator is not None:
            self.phabricator.task_comment(
                self.args.task_id,
                (
                    f"Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} completed:"
                    f"\n{self.spicerack.actions}\n"
                ),
            )
