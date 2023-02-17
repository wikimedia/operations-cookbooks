"""Wipe a kubernetes cluster."""
import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import confirm_on_failure, ask_confirmation, ensure_shell_is_durable

from cumin import nodeset

from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES, PROMETHEUS_MATCHERS

logger = logging.getLogger(__name__)


class WipeK8sCluster(CookbookBase):
    """Wipe a kubernetes cluster.

    This cookbooks automates the procedure to wipe a Kubernetes cluster.
    The idea is to:
    1) Downtime the hosts in the cluster.
    2) Stop kube* daemons across control plane and worker nodes.
    3) Wipe data in the etcd cluster.
    4) Restart kube* daemons and remove downtimes.

    Since the state of a cluster is stored in etcd and the rest is stateless,
    the above procedure should guarantee to start from a clean state.
    """

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments."""
        parser = super().argument_parser()
        parser.add_argument('--reason', required=True, help='Admin reason')
        parser.add_argument(
            '--k8s-cluster', required=True,
            help='K8s cluster to use for downtimes, sanity checks and Cumin aliases',
            choices=ALLOWED_CUMIN_ALIASES.keys())
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
        # List of tuples (alert_host_handle, downtime_id)
        self.downtimes = []

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
            ("workers", self.worker_nodes)
        ]
        for name, remote in components:
            logger.info("Downtime and disable puppet for %s", name)
            alerts = self.spicerack.alerting_hosts(remote.hosts)
            downtime_id = alerts.downtime(
                self.admin_reason, duration=timedelta(minutes=120))
            puppet = self.spicerack.puppet(remote)
            puppet.disable(self.admin_reason)
            self.downtimes.append((alerts, downtime_id))

    def _run_puppet(self):
        components = [
            ("control-plane", self.control_plane_nodes),
            ("workers", self.worker_nodes)
        ]
        for name, remote in components:
            logger.info("Enabling and running puppet on %s nodes...", name)
            puppet = self.spicerack.puppet(remote)
            puppet.run(enable_reason=self.admin_reason)

    def _check_etcd_cluster_status(self):
        logger.info(
            "Checking member list on every node to see if the view "
            "of the cluster is consistent...")
        confirm_on_failure(
            self.etcd_nodes.run_sync,
            "ETCDCTL_API=3 /usr/bin/etcdctl --endpoints https://$(hostname -f):2379 member list"
        )
        ask_confirmation(
            "You should see a consistent response for all nodes in the above "
            "output. Please continue if everything looks good, otherwise "
            "check manually on the nodes before proceeding.")

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"Wipe the K8s cluster {self.k8s_cluster}: {self.args.reason}"

    def run(self) -> int:
        """Required by Spicerack API."""
        # Check the etcd cluster first.
        # If it does not look healthy it's probably not safe to continue
        logger.info("Checking the status of the etcd cluster...")
        self._check_etcd_cluster_status()
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
        all_prom_cluster_alerts = self.spicerack.alertmanager_hosts([".*"], verbatim_hosts=True)
        all_prom_cluster_alerts_id = all_prom_cluster_alerts.downtime(
            self.admin_reason, matchers=PROMETHEUS_MATCHERS[self.k8s_cluster],
            duration=timedelta(minutes=60 * len(affected_nodes)))
        self.downtimes.append((all_prom_cluster_alerts, all_prom_cluster_alerts_id))

        logger.info("Stopping k8s daemons on the control plane nodes...")
        confirm_on_failure(
            self.control_plane_nodes.run_sync, "/usr/bin/systemctl stop 'kube*'"
        )

        logger.info("Stopping k8s daemons on the worker nodes...")
        confirm_on_failure(
            self.worker_nodes.run_sync, "/usr/bin/systemctl stop 'kube*'"
        )

        # Get one etcd node and run the command only on it.
        etcd_node = next(self.etcd_nodes.split(len(self.etcd_nodes)))
        confirm_on_failure(
            etcd_node.run_sync,
            'ETCDCTL_API=3 etcdctl --endpoints https://$(hostname -f):2379 del "" --from-key=true'
        )
        logger.info("Cluster's state wiped!")

        self._run_puppet()

        ask_confirmation(
            "All done. You should re-deploy in-cluster (admin_ng) components now, "
            "next step will be removing downtimes."
        )

        # Remove all downtimes
        for alert_host, downtime_id in self.downtimes:
            alert_host.remove_downtime(downtime_id)
