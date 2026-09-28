"""Upgrade a kubernetes-node package (Kubelet) in place, without reimaging."""

import logging
from argparse import ArgumentParser, Namespace

from spicerack import Spicerack
from spicerack.administrative import Reason
from spicerack.cookbook import LockArgs
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.interactive import ask_confirmation

from cookbooks.sre.k8s import (
    ensure_k8s_services_active,
    K8sBatchBase,
    K8sBatchRunnerBase,
    kubectl_version,
    parse_k8s_version,
)

logger = logging.getLogger(__name__)


class UpgradeKubeletsInplace(K8sBatchBase):
    """Upgrade a kubernetes-node package (Kubelet) in place, without reimaging.

    Cookbook supports upgrade kubelet on worker nodes and control planes using
    K8sBatchRunnerBase batches.

    Worker node action to upgrade kubernetes-node package and do a kubelet restart:

    cookbook sre.k8s.upgrade-kubelets-inplace \
        --k8s-cluster wikikube-eqiad \
        --alias wikikube-worker-eqiad \
        --target-version 1.34.1 \
        --batchsize 1 \
        --reason 'Upgrade Kubernetes worker nodes to 1.34.1' \
        upgrade_kubelets

    Use a control-plane alias to upgrade the kubelet on the control plane.

    Exactly one upgrade action is required.
    """

    batch_default = 1
    batch_max = 6
    valid_actions = ("upgrade_kubelets",)
    argument_reason_required = True

    def argument_parser(self) -> ArgumentParser:
        """Add upgrade options; K8sBatchBase provides --k8s-cluster."""
        parser = super().argument_parser()
        parser.add_argument(
            "--target-version",
            required=True,
            help="Target Kubernetes version, for example 1.34.1",
        )
        return parser

    def get_runner(self, args: Namespace) -> "UpgradeKubeletInplaceRunner":
        """As specified by Spicerack API."""
        return UpgradeKubeletInplaceRunner(args, self.spicerack)


class UpgradeKubeletInplaceRunner(K8sBatchRunnerBase):
    """Upgrade kubernetes-node package and batch restart all kubelets."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        super().__init__(args, spicerack)
        self.remote = spicerack.remote()

        self.control_planes = self.remote.query(f"A:{self.k8s_metadata['control-plane']}")
        self.control_plane_node = next(self.control_planes.split(len(self.control_planes)))
        self.worker_nodes = self.remote.query(f"A:{self.k8s_metadata['workers']}")
        self.target_version = self._args.target_version.removeprefix("v")

    @property
    def runtime_description(self) -> str:
        """Describe the chosen pass."""
        return f"Upgrade kubelet of {self._args.k8s_cluster} to {self.target_version}"

    @property
    def lock_args(self) -> LockArgs:
        """Serialize upgrade passes for this cluster."""
        return LockArgs(suffix=self._args.k8s_cluster, concurrency=1, ttl=21600)


    def validate_node_preconditions(self) -> None:
        """Check installed kubernetes-node package and target version for version skew."""
        try:
            k8s_version = kubectl_version(self.control_plane_node)
            server_version = k8s_version["serverVersion"]["gitVersion"]
        except RuntimeError as error:
            raise RuntimeError(
                f"Failed to get kubernetes version. {error}") from error

        target = parse_k8s_version(self.target_version)
        server = parse_k8s_version(server_version)

        if (
            target.major != server.major
            or target.minor > server.minor
            or server.minor - target.minor > 3
        ):
            message = (
                "Kubelet can be up to 3 minor versions older than the kube-apiserver and "
                "must never be newer than the API server. "
                f"Current API server version {server_version}, requested kubelet version {self.target_version}"
            )
            if self._spicerack.dry_run:
                self.logger.warning("DRY-RUN: would raise: %s", message)
            else:
                raise RuntimeError(message)

        ask_confirmation(
            f"The cluster is running Kubernetes {server_version}. "
            f"Target version: {self.target_version}. "
            "Kubelet can be up to 3 minor versions older than the kube-apiserver and "
            "must never be newer than the API server. "
            "Proceed?"
        )

    def install_node_package(self, hosts: RemoteHosts) -> None:
        """Install kubernetes-node on batch.
        Use --allow-change-held-packages to allow upgrading the hold package.
        Hold is removed after the install
        """
        hosts.run_sync(
            "DEBIAN_FRONTEND=noninteractive /usr/bin/apt-get install -y "
            "-o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold --allow-change-held-packages "
            f"kubernetes-node={self.target_version}-*",
            print_progress_bars=False,
            print_output=False,
        )

    def wait_for_ready(self, hosts: RemoteHosts) -> None:
        """Wait for the target kubelet version and Ready on every batch node."""
        logger.info("Verifying that kubelet services is active...")
        worker_ok = False
        while not worker_ok:
            # ensure_k8s_services_active has an ask_confirmation to abort the check loop
            worker_ok = ensure_k8s_services_active(hosts)

        logger.info("Verifying that node is Ready and has the new version...")
        try:
            for hostname in hosts.hosts:
                # wait until node has updated .status.nodeInfo.kubeletVersion value and is marked Ready
                self.control_plane_node.run_sync(
                    "/usr/bin/kubectl "
                    f"wait node/{hostname} "
                    f"--for=jsonpath='{{.status.nodeInfo.kubeletVersion}}'="
                    f"v{self.target_version} "
                    "--for=condition=Ready "
                    "--timeout=180s",
                    print_progress_bars=False,
                    print_output=False,
                )
        except RemoteExecutionError as error:
            raise RuntimeError("Failed to lookup kubeletVersion and node status on API server") from error

    def restart_daemonset_pods(self, hosts: RemoteHosts) -> None:
        """Deletes daemonset pods to make sure they are re-created by the new kubelet"""
        logger.info("Deleting daemonset pods to force re-creation on new kubelet...")
        for hostname in hosts.hosts:
            try:
                self.control_plane_node.run_sync(
                    "/usr/bin/kubectl "
                    f"delete pod -A --field-selector spec.nodeName={hostname} ",
                    print_progress_bars=False,
                )
            except RemoteExecutionError as error:
                raise RuntimeError(f"Failed to delete daemonset pods on host {hostname}") from error

    def _upgrade_kubelets_action(self, hosts: RemoteHosts, _: Reason) -> None:
        """Called by the inherited batch runner after pre_action drains node."""
        self.install_node_package(hosts)
        self.wait_for_ready(hosts)
        self.restart_daemonset_pods(hosts)

    def run(self) -> int:
        """Run kubelets batch upgrade."""
        self.validate_node_preconditions()
        # K8sBatchRunnerBase pre_action depools, cordons and drains each batch;
        # post_action pools and uncordons after the action.
        result = super().run()
        return result
