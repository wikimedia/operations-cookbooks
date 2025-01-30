"""Wipe a kubernetes cluster."""

import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from typing import Union

from cumin import nodeset
from spicerack import Spicerack
from spicerack.alerting import AlertingHosts
from spicerack.alertmanager import Alertmanager
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import (
    ask_confirmation,
    ask_input,
    confirm_on_failure,
    ensure_shell_is_durable,
)

from cookbooks.sre.k8s import (
    ALLOWED_CUMIN_ALIASES,
    PROMETHEUS_MATCHERS,
    etcd_cluster_healthy,
    etcdctl,
)

logger = logging.getLogger(__name__)


def ask_yesno(message: str) -> bool:
    """Ask the user for a yes/no answer in interactive mode.

    Examples:
        ::
            >>> ask_yesno('Ready to continue?')
            ==> Ready to continue?
            Type "yes" or "no"

    Arguments:
        message (str): the message to be printed before asking for confirmation.

    Returns:
        bool: :py:data:`True` if the user answered "yes", :py:data:`False` otherwise.

    """
    response = ask_input("\n".join((message, 'Type "yes" or "no"')), ["yes", "no"])
    return response == "yes"


class WipeK8sCluster(CookbookBase):
    """Wipe a kubernetes cluster.

    This cookbooks automates the procedure to wipe a Kubernetes cluster.
    The idea is to:
    1) Downtime the hosts in the cluster.
    2) Stop kube* daemons across control plane and worker nodes.
    3) Wipe data in the etcd cluster.
    4) Optional: Restart kube* daemons and remove downtimes by running puppet.

    Since the state of a cluster is stored in etcd and the rest is stateless,
    the above procedure should guarantee to start from a clean state.
    """

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments."""
        parser = super().argument_parser()
        parser.add_argument("--reason", required=True, help="Admin reason")
        parser.add_argument(
            "-M",
            "--minutes",
            type=int,
            default=0,
            help="For how many minutes the downtime should last. [optional, default=0]",
        )
        parser.add_argument(
            "-H",
            "--hours",
            type=int,
            default=0,
            help="For how many hours the downtime should last. [optional, default=0]",
        )
        parser.add_argument(
            "-D",
            "--days",
            type=int,
            default=0,
            help="For how many days the downtime should last. [optional, default=0]",
        )
        parser.add_argument(
            "--k8s-cluster",
            required=True,
            help="K8s cluster to use for downtimes, sanity checks and Cumin aliases",
            choices=ALLOWED_CUMIN_ALIASES.keys(),
        )
        return parser

    def get_runner(self, args: Namespace) -> "WipeK8sClusterRunner":
        """As specified by Spicerack API."""
        return WipeK8sClusterRunner(args, self.spicerack)


class WipeK8sClusterRunner(CookbookRunnerBase):
    """Wipe a kubernetes cluster cookbook runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self.args = args
        self.spicerack = spicerack
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.k8s_cluster = args.k8s_cluster
        self.spicerack_remote = self.spicerack.remote()
        self.etcd_nodes = self.spicerack_remote.query(self.etcd_query)
        self.control_plane_nodes = self.spicerack_remote.query(self.control_plane_query)
        self.worker_nodes = self.spicerack_remote.query(self.workers_query)
        if args.minutes == args.hours == args.days == 0:
            self.downtime_duration = timedelta(hours=2)
        else:
            self.downtime_duration = timedelta(
                days=args.days, hours=args.hours, minutes=args.minutes
            )
        # List of tuples (alert_host_handle, downtime_id)
        self.downtimes: list[tuple[Union[Alertmanager, AlertingHosts], str]] = []

    @property
    def etcd_query(self):
        """Returns a safe Cumin query to use for etcd nodes"""
        return f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['etcd']}"

    @property
    def control_plane_query(self):
        """Returns a safe Cumin query to use for control plane nodes"""
        return f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['control-plane']}"

    @property
    def workers_query(self):
        """Returns a safe Cumin query to use for worker nodes"""
        return f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['workers']}"

    def _prepare_nodes(self):
        """Downtime and disable puppet on all components"""
        components = [
            ("control-plane", self.control_plane_nodes),
            ("workers", self.worker_nodes),
        ]
        for name, remote in components:
            logger.info("Downtime and disable puppet for %s", name)
            alerts = self.spicerack.alerting_hosts(remote.hosts)
            downtime_id = alerts.downtime(
                self.admin_reason, duration=self.downtime_duration
            )
            puppet = self.spicerack.puppet(remote)
            puppet.disable(self.admin_reason)
            self.downtimes.append((alerts, downtime_id))

    def _run_puppet(self):
        components = [
            ("control-plane", self.control_plane_nodes),
            ("workers", self.worker_nodes),
        ]
        for name, remote in components:
            logger.info("Enabling and running puppet on %s nodes...", name)
            puppet = self.spicerack.puppet(remote)
            confirm_on_failure(puppet.run, enable_reason=self.admin_reason)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"Wipe the K8s cluster {self.k8s_cluster}: {self.args.reason}"

    def run(self) -> None:
        """Required by Spicerack API."""
        # Check the etcd cluster first.
        # If it does not look healthy it's probably not safe to continue
        logger.info("Checking the status of the etcd cluster...")
        # Get one etcd node to run commands on
        etcd_node = next(self.etcd_nodes.split(len(self.etcd_nodes)))
        if not etcd_cluster_healthy(etcd_node):
            ask_confirmation(
                "etcd cluster is in an unhealthy state. "
                "Do you want to continue anyway?"
            )
        self._prepare_nodes()
        affected_nodes = nodeset()
        affected_nodes.update(self.control_plane_nodes.hosts)
        affected_nodes.update(self.worker_nodes.hosts)

        ask_confirmation(
            f"The cookbook is going to wipe the K8s cluster {self.k8s_cluster}. "
            "Is it ok to proceed?"
        )

        # Add an extra downtime for the whole Prometheus k8s cluster
        # to reduce the noise as much as possible.
        all_prom_cluster_alerts = self.spicerack.alertmanager()
        all_prom_cluster_alerts_id = all_prom_cluster_alerts.downtime(
            self.admin_reason,
            matchers=PROMETHEUS_MATCHERS[self.k8s_cluster],
            duration=timedelta(minutes=60 * len(affected_nodes)),
        )
        self.downtimes.append((all_prom_cluster_alerts, all_prom_cluster_alerts_id))

        # In addition to k8s daemons, we need to stop confd-k8s which would (re-)start them
        # when we re-publish the service account certificates to etcd later.
        logger.info("Stopping k8s daemons and confd-k8s on the control plane nodes...")
        confirm_on_failure(
            self.control_plane_nodes.run_sync,
            "/usr/bin/systemctl stop 'kube*.service' confd-k8s.service",
            print_progress_bars=False,
        )

        logger.info("Stopping k8s daemons on the worker nodes...")
        confirm_on_failure(
            self.worker_nodes.run_sync,
            "/usr/bin/systemctl stop 'kube*.service'",
            print_progress_bars=False,
        )

        confirm_on_failure(
            etcd_node.run_sync,
            etcdctl('del "" --from-key=true'),
            print_progress_bars=False,
        )
        logger.info("Cluster's state wiped!")

        # Re-publish the service account certificates to etcd
        confirm_on_failure(
            self.control_plane_nodes.run_sync,
            "/usr/bin/systemctl restart 'kube-publish-sa-cert.service'",
            print_progress_bars=False,
        )

        # The user might decide to run puppet manually in order to have more control
        if ask_yesno(
            "Cluster's state has been wiped. "
            "Do you want me to run puppet on all cluster nodes now?",
        ):
            self._run_puppet()

        if ask_yesno(
            "After running puppet, all nodes will have rejoined the cluster cordoned. "
            "Do you want me to uncordon all of them?"
        ):
            # Get one control plane node and run the command only on it.
            ctrl_node = next(
                self.control_plane_nodes.split(len(self.control_plane_nodes))
            )
            nodes = " ".join(affected_nodes)
            confirm_on_failure(
                ctrl_node.run_sync,
                f"/usr/bin/kubectl uncordon {nodes}",
                print_progress_bars=False,
            )
            logger.info("Uncordoned: %s", nodes)

        ask_confirmation(
            "You should re-deploy in-cluster (admin_ng) components now, "
            "next step will be removing downtimes."
        )

        # Remove all downtimes
        for alert_host, downtime_id in self.downtimes:
            alert_host.remove_downtime(downtime_id)
