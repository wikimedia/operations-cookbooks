"""WMCS Toolforge - Add a new k8s worker node to a toolforge installation.

Usage example:
    cookbook wmcs.toolforge.add_k8s_worker_node \
        --project toolsbeta \
        --worker-prefix toolsbeta-k8s-test-worker

"""
# pylint: disable=too-many-arguments
import argparse
import datetime
import logging
from typing import Optional

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs import (
    CommonOpts,
    KubeadmController,
    KubernetesController,
    OpenstackAPI,
    OpenstackServerGroupPolicy,
    add_common_opts,
    dologmsg,
    with_common_opts,
)
from cookbooks.wmcs.vps.create_instance_with_prefix import CreateInstanceWithPrefix
from cookbooks.wmcs.vps.refresh_puppet_certs import RefreshPuppetCerts

LOGGER = logging.getLogger(__name__)


class ToolforgeAddK8sWorkerNode(CookbookBase):
    """WMCS Toolforge cookbook to add a new worker node"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser, project_default="toolsbeta")
        parser.add_argument(
            "--k8s-worker-prefix",
            required=False,
            default=None,
            help="Prefix for the k8s worker nodes, default is <project>-k8s-worker.",
        )
        parser.add_argument(
            "--k8s-control-prefix",
            required=False,
            default=None,
            help="Prefix for the k8s control nodes, default is the k8s_worker_prefix replacing 'worker' by 'control'.",
        )
        parser.add_argument(
            "--flavor",
            required=False,
            default=None,
            help=(
                "Flavor for the new instance (will use the same as the latest existing one by default, ex. "
                "g2.cores4.ram8.disk80, ex. 06c3e0a1-f684-4a0c-8f00-551b59a518c8)."
            ),
        )
        parser.add_argument(
            "--image",
            required=False,
            default=None,
            help=(
                "Image for the new instance (will use the same as the latest existing one by default, ex. "
                "debian-10.0-buster, ex. 64351116-a53e-4a62-8866-5f0058d89c2b)"
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(args, ToolforgeAddK8sWorkerNodeRunner,)(
            k8s_worker_prefix=args.k8s_worker_prefix,
            k8s_control_prefix=args.k8s_control_prefix,
            image=args.image,
            flavor=args.flavor,
            spicerack=self.spicerack,
        )


class ToolforgeAddK8sWorkerNodeRunner(CookbookRunnerBase):
    """Runner for ToolforgeAddK8sWorkerNode"""

    def __init__(
        self,
        common_opts: CommonOpts,
        k8s_worker_prefix: Optional[str],
        k8s_control_prefix: Optional[str],
        spicerack: Spicerack,
        image: Optional[str] = None,
        flavor: Optional[str] = None,
    ):
        """Init"""
        self.common_opts = common_opts
        self.k8s_worker_prefix = k8s_worker_prefix
        self.k8s_control_prefix = k8s_control_prefix
        self.spicerack = spicerack
        self.image = image
        self.flavor = flavor

    def run(self) -> Optional[int]:
        """Main entry point"""
        dologmsg(common_opts=self.common_opts, message="Adding a new k8s worker node")
        k8s_worker_prefix = (
            self.k8s_worker_prefix if self.k8s_worker_prefix is not None else f"{self.common_opts.project}-k8s-worker"
        )
        k8s_control_prefix = (
            self.k8s_control_prefix
            if self.k8s_control_prefix is not None
            else k8s_worker_prefix.replace("worker", "control")
        )

        start_args = [
            "--prefix",
            k8s_worker_prefix,
            "--security-group",
            f"{self.common_opts.project}-k8s-full-connectivity",
            "--server-group",
            self.k8s_worker_prefix,
            "--server-group-policy",
            OpenstackServerGroupPolicy.SOFT_ANTI_AFFINITY.value,
        ] + self.common_opts.to_cli_args()

        if self.image:
            start_args.extend(["--image", self.image])

        if self.flavor:
            start_args.extend(["--flavor", self.flavor])

        create_instance_cookbook = CreateInstanceWithPrefix(spicerack=self.spicerack)
        new_member = create_instance_cookbook.get_runner(
            args=create_instance_cookbook.argument_parser().parse_args(start_args)
        ).run()
        node = self.spicerack.remote().query(f"D{{{new_member.server_fqdn}}}", use_sudo=True)

        device = "/dev/sdb"
        LOGGER.info("Making sure %s is ext4, docker ovelay storage needs it", device)
        node.run_sync(
            # we have to remove the mount from fstab as the fstype will be wrong
            Command(
                f"grep '{device}.*ext4' /proc/mounts "
                "|| { "
                f"    sudo umount {device} 2>/dev/null; "
                f"    sudo -i mkfs.ext4 {device}; "
                f"    sudo sed -i -e '\\|^.*/var/lib/docker\\s.*|d' /etc/fstab; "
                "}"
            )
        )

        LOGGER.info("Making sure that the proper puppetmaster is setup for the new node %s", new_member.server_fqdn)
        LOGGER.info("It might fail before rebooting, will make sure it runs after too.")
        refresh_puppet_certs_cookbook = RefreshPuppetCerts(spicerack=self.spicerack)
        refresh_puppet_certs_cookbook.get_runner(
            args=refresh_puppet_certs_cookbook.argument_parser().parse_args(
                ["--fqdn", new_member.server_fqdn, "--pre-run-puppet", "--ignore-failures"]
            ),
        ).run()

        LOGGER.info(
            (
                "Rebooting worker node %s to make sure iptables alternatives "
                "are taken into account by docker, kube-proxy and calico."
            ),
            new_member.server_fqdn,
        )
        reboot_time = datetime.datetime.utcnow()
        node.reboot()
        node.wait_reboot_since(since=reboot_time)

        LOGGER.info(
            "Rebooted node %s, running puppet again, this time it should work.",
            new_member.server_fqdn,
        )
        PuppetHosts(remote_hosts=node).run()

        LOGGER.info("Getting the list of k8s control nodes for the project...")
        openstack_api = OpenstackAPI(remote=self.spicerack.remote(), project=self.common_opts.project)
        all_nodes = openstack_api.server_list()
        k8s_control_node_hostname = next(
            node["Name"] for node in all_nodes if node["Name"].startswith(k8s_control_prefix)
        )

        kubeadm = KubeadmController(remote=self.spicerack.remote(), controlling_node_fqdn=new_member.server_fqdn)
        # guessing that the domain of the k8s and kubeadmin are the same
        k8s_control_node_fqdn = f"{k8s_control_node_hostname}.{kubeadm.get_nodes_domain()}"
        kubectl = KubernetesController(remote=self.spicerack.remote(), controlling_node_fqdn=k8s_control_node_fqdn)
        LOGGER.info("Joining the cluster...")
        kubeadm.join(kubernetes_controller=kubectl, wait_for_ready=True)

        dologmsg(
            common_opts=self.common_opts, message=f"Added a new k8s worker {new_member.server_fqdn} to the worker pool"
        )
