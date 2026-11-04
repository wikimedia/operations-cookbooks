import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.icinga import IcingaError
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.decorators import retry
from wmflib.interactive import AbortError, ask_confirmation, ensure_shell_is_durable

from cookbooks.sre import Results
from cookbooks.sre.k8s import (
    ALLOWED_CUMIN_ALIASES,
    kubectl_version,
)

logger = logging.getLogger(__name__)


class UpgradeAPIServersInplace(CookbookBase):
    """Upgrade the kubernetes-master package in place, without reimaging.

    This cookbook upgrades the kubernetes-master package on control planes.

    cookbook sre.k8s.upgrade-apiservers-inplace \
        --k8s-cluster wikikube-eqiad \
        --target-version 1.34.1 \
        --reason 'Upgrade Kubernetes control planes to 1.34.1' \
        upgrade_apiservers

    Exactly one upgrade action is required.
    """

    argument_reason_required = True
    argument_task_required = True

    def argument_parser(self) -> ArgumentParser:
        """Add k8s-cluster and target-version arguments."""
        parser = super().argument_parser()
        parser.add_argument(
            "--k8s-cluster",
            required=True,
            help="K8s cluster to use as a sanity check and for Cumin aliases.",
            choices=ALLOWED_CUMIN_ALIASES.keys(),
        )

        parser.add_argument(
            "--target-version",
            required=True,
            help="Target Kubernetes version, for example 1.34.1",
        )
        return parser

    def get_runner(self, args: Namespace) -> "UpgradeAPIServersInplaceRunner":
        """As specified by Spicerack API."""
        return UpgradeAPIServersInplaceRunner(args, self.spicerack)


class UpgradeAPIServersInplaceRunner(CookbookRunnerBase):
    """Upgrade kubernetes-master package on control planes."""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        ensure_shell_is_durable()
        self._args = args
        self.k8s_metadata = ALLOWED_CUMIN_ALIASES[args.k8s_cluster]
        self.target_version = args.target_version.removeprefix("v")
        self._spicerack = spicerack
        self.spicerack_remote = spicerack.remote()
        self.reason = spicerack.admin_reason(args.reason)
        self.dry_run = spicerack.dry_run

        self.all_control_planes = self.spicerack_remote.query(f"A:{self.k8s_metadata['control-plane']}")
        # Select one control plane that will keep running and receive the upgrade first
        self.first_control_plane = next(self.all_control_planes.split(len(self.all_control_planes)))
        self.remaining_control_planes = self.spicerack_remote.query(
            f"A:{self.k8s_metadata['control-plane']} and not P{{{self.first_control_plane.hosts[0]}}}"
        )
        self.results = Results(action="upgrade-apiservers", hosts=self.all_control_planes.hosts)

    @property
    def runtime_description(self) -> str:
        """Describe the chosen pass."""
        return f"Upgrade apiservers of {self._args.k8s_cluster} to {self.target_version}"

    @property
    def lock_args(self) -> LockArgs:
        """Serialize upgrade passes for this cluster."""
        return LockArgs(suffix=self._args.k8s_cluster, concurrency=1, ttl=21600)

    def install_package(self, hosts: RemoteHosts, package_name) -> None:
        """Install specified debian package with self.target_version on hosts."""
        # Use --allow-change-held-packages to allow upgrading the hold package.
        # Hold is removed after the install
        hosts.run_sync(
            "DEBIAN_FRONTEND=noninteractive /usr/bin/apt-get install -y "
            "-o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold --allow-change-held-packages "
            f"{package_name}={self.target_version}-*",
            is_safe=False,
            print_progress_bars=False,
            print_output=False,
        )

    def hold_package(self, hosts: RemoteHosts) -> None:
        """Apply hold on kubernetes-node and kubernetes-master package."""
        hosts.run_sync(
            "apt-mark hold kubernetes-node kubernetes-master",
            is_safe=False,
            print_progress_bars=False,
            print_output=False,
        )

    def restart_apiserver(self, hosts: RemoteHosts) -> None:
        """Restart apiserver on hosts.

        The kubernetes-master package will restart kube-controller-manager and kube-scheduler
        automatically. The apiserver needs to be restarted manually.
        """
        hosts.run_sync(
            "/usr/bin/systemctl restart kube-apiserver.service",
            is_safe=False,
            print_progress_bars=False,
            print_output=False,
        )

    def stop_apiserver(self, hosts: RemoteHosts) -> None:
        """Stop all apiserver components on hosts."""
        units = ["kube-apiserver.service", "kube-controller-manager.service", "kube-scheduler.service"]
        hosts.run_sync(
            f"/usr/bin/systemctl stop {' '.join(units)}",
            is_safe=False,
            print_progress_bars=False,
            print_output=False,
        )

    @retry(  # pylint: disable=no-value-for-parameter
        tries=10,
        delay=timedelta(seconds=5),
        backoff_mode="constant",
        failure_message="apiserver not running expected target version",
        exceptions=(RuntimeError,),
    )
    def wait_for_ready(self, hosts: RemoteHosts) -> None:
        """Wait for the target apiserver to be ready."""
        try:
            for remote in hosts:
                k8s_version = kubectl_version(remote)
                server_version = k8s_version["serverVersion"]["gitVersion"]
                if server_version != f"v{self.target_version}":
                    msg = f"API server on {remote} is not at target version {self.target_version}"
                    if self.dry_run:
                        logger.info(msg)
                    else:
                        raise RuntimeError(msg)
        except RemoteExecutionError as error:
            raise RuntimeError("Failed to lookup API server version") from error

    def pool_depool(self, hosts: RemoteHosts, action: str) -> None:
        """Pool or depool the specified nodes."""
        if action not in ("pool", "depool"):
            raise ValueError(f"Invalid action: {action}")
        pool_args = ["--k8s-cluster", self._args.k8s_cluster, "--force", action, f"{str(hosts)}"]
        ret = self._spicerack.run_cookbook("sre.k8s.pool-depool-node", pool_args)
        if ret:
            ask_confirmation(f"Failed to {action} nodes. Please verify manually.")

    def run_upgrade(self) -> None:
        """
        This will:
        1. Depool all but the first control-plane node.
        2. Apply hold kubernetes-node and kubernetes-master package
        3. Run puppet on all control-plane nodes.
        4. Disable puppet on all control-plane nodes.
        5. Stop all apiserver components on all but one control-plane node.
        6. Install the new kubernetes-master package on the remaining control-plane node.
        7. Restart the apiserver on the upgraded control-plane node.
        8. Wait for the apiserver to be ready.
        9. Install the new kubernetes-master package on the remaining control-plane nodes.
        10. Restart the apiserver on the remaining control-plane nodes.
        11. Wait for the apiserver to be ready on all control-plane nodes.
        12. Re-enable and run puppet on all control-plane nodes.
        13. Pool the remaining control-plane nodes.
        """
        # Depool all but the first control-plane since we stop apiserver components on them
        self.pool_depool(self.remaining_control_planes, action="depool")

        # Hold kubernetes-node and kubernetes-master package.
        # The first puppet run after switching the kubernetes version will install a new kubernetes-clientXY package.
        # That apt-get install call would also update kubernetes-master and kubernetes-node since both
        # will be available from the new apt component.
        self.hold_package(self.all_control_planes)

        ask_confirmation(
            "At this point you should puppet-merge the hieradata/common/kubernetes.yaml change updating the k8s version"
        )
        # Run puppet on all control-plane nodes to enable the new apt components
        self._spicerack.puppet(self.all_control_planes).run()
        # Disable puppet to prevent it from restarting services
        self._spicerack.puppet(self.all_control_planes).disable(self.reason)
        # Stop all apiserver components on all but the first control-plane node
        self.stop_apiserver(self.remaining_control_planes)

        logger.info("Installing kubernetes-master package on the first control-plane")
        self.install_package(self.first_control_plane, "kubernetes-master")
        logger.info("Restarting apiserver on the first control-plane")
        self.restart_apiserver(self.first_control_plane)
        # Validate the first control-plane is ready
        self.wait_for_ready(self.first_control_plane)

        # At this point the kubernetes cluster has practically been upgraded.
        # Now update the remaining control-plane nodes and ensure the apiservers run the expected version
        logger.info("Installing kubernetes-master package on the remaining control-planes")
        self.install_package(self.remaining_control_planes, "kubernetes-master")
        self.restart_apiserver(self.remaining_control_planes)
        self.wait_for_ready(self.remaining_control_planes)

        # All done, re-enable puppet and run on all control-planes
        logger.info("Re-enabling and running puppet on all control-planes")
        self._spicerack.puppet(self.all_control_planes).run(enable_reason=self.reason)
        # Repool
        self.pool_depool(self.remaining_control_planes, action="pool")

    def run(self) -> None:
        """Downtime, then run_upgrade"""
        icinga_hosts = self._spicerack.icinga_hosts(self.all_control_planes.hosts)
        alerting_hosts = self._spicerack.alerting_hosts(self.all_control_planes.hosts)
        try:
            duration = timedelta(minutes=20)
            with alerting_hosts.downtimed(self.reason, duration=duration):
                self.run_upgrade()
                icinga_hosts.wait_for_optimal(skip_acked=True)
            self.results.success(self.all_control_planes.hosts)
        except IcingaError as error:
            ask_confirmation(f"Failed to downtime hosts: {error}")
            logger.warning(error)

        except AbortError as error:
            # Some host failed to come up again, or something fundamental broke.
            # log an error and raise *without* repooling
            logger.error(error)
            logger.error(
                "Error %s: Hosts %s, they may still be depooled",
                self._args.action,
                self.all_control_planes,
            )
            self.results.fail(self.all_control_planes.hosts)
            raise
