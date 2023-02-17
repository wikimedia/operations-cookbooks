"""Upgrade a kubernetes cluster to a new version."""
import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from cumin import nodeset

from cookbooks.sre.hosts import OS_VERSIONS
from cookbooks.sre.k8s import ALLOWED_CUMIN_ALIASES, PROMETHEUS_MATCHERS

logger = logging.getLogger(__name__)


class UpgradeK8sCluster(CookbookBase):
    """Upgrade a kubernetes cluster to a new version.

    This cookbooks automates the reimage of kubernetes and etcd nodes related
    to a certain cluster, to allow a quick and smooth migration to a new k8s
    version.

    The high level workflow is the following:
    - Downtime all hosts involved.
    - Disable puppet, stop etcd and k8s daemons on nodes.
    - If etcd is specified, reimage all nodes indicated by the alias.
    - If the control plane alias is specified, reimage all the nodes indicated
      by the alias.
    - Finally, reimage all the worker nodes.

    Once all nodes are up and running the new cluster should be running
    with base functionalities.

    Note: for the moment the cookbook does not take into consideration any
    pool/depool action for traffic flowing to the cluster. Please check
    sre.k8s.pool-depool-cluster.py instead.

    In theory to successfully upgrade a cluster we'd need to wipe all nodes,
    but there may be reasons to force the cookbook to run only on a subset
    of them, so we keep only the worker nodes as mandatory.
    """

    def argument_parser(self) -> ArgumentParser:
        """Parse the command line arguments."""
        parser = super().argument_parser()
        parser.add_argument('--reason', required=True, help='Admin reason')
        parser.add_argument(
            '--k8s-cluster', required=True,
            help='K8s cluster to use for downtimes, sanity checks and Cumin aliases',
            choices=ALLOWED_CUMIN_ALIASES.keys())
        parser.add_argument(
            '--etcd-query', required=False,
            help=(
                'Optional Cumin query to use a subset of the etcd nodes '
                '(You need to use the Cumin global grammar, see '
                'https://wikitech.wikimedia.org/wiki/Cumin#Global_grammar_host_selection)'
            )
        )
        parser.add_argument(
            '--control-plane-query', required=False,
            help=(
                'Optional Cumin query to use a subset of the control plane nodes '
                '(You need to use the Cumin global grammar, see '
                'https://wikitech.wikimedia.org/wiki/Cumin#Global_grammar_host_selection)'
            )
        )
        parser.add_argument(
            '--workers-query', required=False,
            help=(
                'Optional Cumin query to use a subset of the worker nodes '
                '(You need to use the Cumin global grammar, see '
                'https://wikitech.wikimedia.org/wiki/Cumin#Global_grammar_host_selection)'
            )
        )
        parser.add_argument(
            '--os',
            required=True,
            help='Debian OS codename to use for the reimages',
            choices=OS_VERSIONS)
        etcd_group = parser.add_mutually_exclusive_group()
        etcd_group.add_argument(
            '--etcd-wipe-only',
            help='Wipe data on etcd without reimage',
            action='store_true', default=False)
        etcd_group.add_argument(
            '--skip-etcd',
            help='Skip etcd nodes',
            action='store_true', default=False)
        parser.add_argument(
            '--skip-control-plane',
            help='Skip control plane nodes',
            action='store_true', default=False)
        parser.add_argument(
            '--skip-workers',
            help='Skip worker nodes',
            action='store_true', default=False)

        return parser

    def get_runner(self, args: Namespace) -> "UpgradeK8sClusterRunner":
        """As specified by Spicerack API."""
        return UpgradeK8sClusterRunner(args, self.spicerack)


class UpgradeK8sClusterRunner(CookbookRunnerBase):
    """Upgrade a kubernetes cluster cookbook runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self.args = args
        self.spicerack = spicerack
        self.admin_reason = spicerack.admin_reason(args.reason)
        self.k8s_cluster = args.k8s_cluster
        self.spicerack_remote = self.spicerack.remote()

        if not self.args.skip_etcd:
            self.etcd_nodes = self.spicerack_remote.query(self.etcd_query)
        else:
            self.etcd_nodes = None
        if not self.args.skip_control_plane:
            self.control_plane_nodes = self.spicerack_remote.query(self.control_plane_query)
        else:
            self.control_plane_nodes = None
        if not self.args.skip_workers:
            self.worker_nodes = self.spicerack_remote.query(self.workers_query)
        else:
            self.worker_nodes = None

        # List of tuples (alert_host_handle, downtime_id)
        self.downtimes = []

    @property
    def etcd_query(self):
        """Returns a safe Cumin query to use for etcd nodes"""
        query = f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['etcd']}"
        if self.args.etcd_query:
            query = f"{query} and {self.args.etcd_query}"
        return query

    @property
    def control_plane_query(self):
        """Returns a safe Cumin query to use for control plane nodes"""
        query = f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['control-plane']}"
        if self.args.control_plane_query:
            query = f"{query} and {self.args.control_plane_query}"
        return query

    @property
    def workers_query(self):
        """Returns a safe Cumin query to use for worker nodes"""
        query = f"A:{ALLOWED_CUMIN_ALIASES[self.k8s_cluster]['workers']}"
        if self.args.workers_query:
            query = f"{query} and {self.args.workers_query}"
        return query

    def _prepare_nodes(self, total_hosts):
        """Downtime and disable puppet on all components"""
        components = [
            ("etcd", self.etcd_nodes),
            ("control-plane", self.control_plane_nodes),
            ("workers", self.worker_nodes)
        ]
        for name, remote in components:
            if remote:
                logger.info(
                    "Downtime and disable puppet for %s", name
                )
                alerts = self.spicerack.alerting_hosts(remote.hosts)
                downtime_id = alerts.downtime(
                    self.admin_reason, duration=timedelta(minutes=120 * total_hosts))
                puppet = self.spicerack.puppet(remote)
                puppet.disable(self.admin_reason)
                self.downtimes.append((alerts, downtime_id))
            else:
                ask_confirmation(
                    f"Skipping {name}, assuming that they are either already "
                    "wiped or reimaged in a separate step. If this is not the case, "
                    "please restart the cookbook with a different set of args."
                )

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
        return f"Upgrade K8s version: {self.args.reason}"

    def run(self) -> int:  # pylint: disable=too-many-branches
        """Required by Spicerack API."""
        affected_nodes = nodeset()
        if self.etcd_nodes:
            affected_nodes.update(self.etcd_nodes.hosts)
        if self.control_plane_nodes:
            affected_nodes.update(self.control_plane_nodes.hosts)
        if self.worker_nodes:
            affected_nodes.update(self.worker_nodes.hosts)

        ask_confirmation(
            "The cookbook is going to make destructive actions (like reimage) "
            f"to the following nodes:\n{affected_nodes}.\nIs it ok to proceed?"
        )

        self._prepare_nodes(len(affected_nodes))

        # Add an extra downtime for the whole Prometheus k8s cluster
        # to reduce the noise as much as possible.
        all_prom_cluster_alerts = self.spicerack.alertmanager_hosts([".*"], verbatim_hosts=True)
        all_prom_cluster_alerts_id = all_prom_cluster_alerts.downtime(
            self.admin_reason, matchers=PROMETHEUS_MATCHERS[self.k8s_cluster],
            duration=timedelta(minutes=120 * len(affected_nodes)))
        self.downtimes.append((all_prom_cluster_alerts, all_prom_cluster_alerts_id))

        if self.control_plane_nodes:
            logger.info("Stopping k8s daemons in the control plane...")
            confirm_on_failure(
                self.control_plane_nodes.run_sync, "/usr/bin/systemctl stop 'kube*'"
            )

        if self.worker_nodes:
            logger.info("Stopping k8s daemons on the workers...")
            confirm_on_failure(
                self.worker_nodes.run_sync, "/usr/bin/systemctl stop 'kube*'"
            )

        # The procedure that we follow for etcd is:
        # 1) Wipe the cluster from current data.
        # 2) If needed, reimage one node at the time
        # We tried in the past to stop all etcd daemons and reimage one
        # node at the time, but it turned out to be more difficult than
        # expected since we use discovery records and the bootstrap/init
        # procedure is not straightforward.
        if self.etcd_nodes:
            self._check_etcd_cluster_status()
            # Get one etcd node and run the command only on it.
            etcd_node = next(self.etcd_nodes.split(len(self.etcd_nodes)))
            ask_confirmation(
                f"Going to wipe the etcd v2/v3 endpoints on {etcd_node}.")
            # v2 API
            confirm_on_failure(
                etcd_node.run_sync,
                'etcdctl -C https://$(hostname -f):2379 rm -r /calico'
            )
            # v3 API
            confirm_on_failure(
                etcd_node.run_sync,
                'ETCDCTL_API=3 etcdctl --endpoints https://$(hostname -f):2379 del "" --from-key=true'
            )

        logger.info(
            "All cluster components stopped or wiped!")

        ask_confirmation(
            "You may need to merge a puppet change to upgrade the K8s version, "
            "if so please do it now and proceed."
        )

        if self.etcd_nodes:
            if not self.args.etcd_wipe_only:
                ask_confirmation(
                    f"The etcd hosts to reimage are {self.etcd_nodes.hosts}. \n"
                    "Does the list look good? ")
                for host in self.etcd_nodes.hosts:
                    # We assume that they are all Ganeti nodes
                    return_code = self.spicerack.run_cookbook(
                        "sre.ganeti.reimage",
                        ["--no-downtime", "--os", self.args.os, host.split(".")[0]]
                    )
                    if return_code:
                        ask_confirmation(
                            "The cookbook returned a non-zero code, something "
                            "failed and you'd need to check. Do you want to "
                            "continue anyway?"
                        )
                logger.info("etcd nodes reimaged!")
            else:
                logger.info("Re-enabling puppet on etcd nodes...")
                puppet = self.spicerack.puppet(self.etcd_nodes)
                puppet.enable(self.admin_reason)

            self._check_etcd_cluster_status()

        if self.control_plane_nodes:
            ask_confirmation(
                f"The control plane hosts to reimage are {self.control_plane_nodes.hosts}. \n"
                "Does the list look good? Please note: we assume that they are "
                "Ganeti node, abort if it is not the case.")
            for host in self.control_plane_nodes.hosts:
                # We assume that they are all Ganeti nodes.
                return_code = self.spicerack.run_cookbook(
                    "sre.ganeti.reimage",
                    ["--no-downtime", "--os", self.args.os, host.split(".")[0]]
                )
                if return_code:
                    ask_confirmation(
                        "The cookbook returned a non-zero code, something "
                        "failed and you'd need to check. Do you want to "
                        "continue anyway?"
                    )
            logger.info("Control plane nodes reimaged! "
                        "Checking on every node to see if the view of the cluster is "
                        "consistent...")
            confirm_on_failure(
                self.control_plane_nodes.run_sync,
                "/usr/bin/kubectl --kubeconfig=/etc/kubernetes/admin.conf cluster-info"
            )
            ask_confirmation(
                "You should see a consistent response for all nodes in the above "
                "output. Please continue if everything looks good, otherwise "
                "check manually on the nodes before proceeding.")

        if self.worker_nodes:
            ask_confirmation(
                f"The worker hosts to reimage are {self.worker_nodes.hosts}. \n"
                "Does the list look good?")
            for host in self.worker_nodes.hosts:
                hostname = host.split(".")[0]
                netbox_server = self.spicerack.netbox_server(hostname)
                if netbox_server.virtual:
                    cookbook_name = "sre.ganeti.reimage"
                else:
                    cookbook_name = "sre.hosts.reimage"
                return_code = self.spicerack.run_cookbook(
                    cookbook_name,
                    ["--no-downtime", "--os", self.args.os, hostname]
                )
                if return_code:
                    ask_confirmation(
                        "The cookbook returned a non-zero code, something "
                        "failed and you'd need to check. Do you want to "
                        "continue anyway?"
                    )

            logger.info("Worker nodes reimaged!")

        # Remove all downtimes
        for alert_host, downtime_id in self.downtimes:
            alert_host.remove_downtime(downtime_id)
