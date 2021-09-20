"""WMCS Toolforge - Add an existing etcd node to an existing etcd cluster.

Note that if the node is already part of the cluster, this cookbook will still
work (might refresh puppet certs though, and restart services).

Usage example:
    cookbook wmcs.toolforge.etcd.add_node_to_cluster \
        --project toolsbeta \
        --fqdn-to-add toolsbeta-k8s-

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import base64
import logging
import time
from typing import List, Optional

import yaml
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import Remote, RemoteHosts

from cookbooks.wmcs import natural_sort_key, simple_create_file
from cookbooks.wmcs.toolforge.etcd.add_node_to_hiera import AddNodeToHiera
from cookbooks.wmcs.vps.refresh_puppet_certs import RefreshPuppetCerts

LOGGER = logging.getLogger(__name__)


class AddNodeToCluster(CookbookBase):
    """WMCS Toolforge cookbook to add an existing etcd node to the cluster (and related configs)."""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--etcd-prefix",
            required=False,
            default=None,
            help=("Prefix for the k8s etcd nodes, default is <project>-k8s-etcd"),
        )
        parser.add_argument(
            "--new-member-fqdn",
            required=True,
            help=("Fully quialified domain name of the member to add."),
        )
        parser.add_argument(
            "--skip-puppet-bootstrap",
            action="store_true",
            help=(
                "Skip all the puppet bootstraping section, useful if you "
                "already did it and you are rerunning, or if you did it "
                "manually"
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return AddNodeToClusterRunner(
            etcd_prefix=args.etcd_prefix,
            new_member_fqdn=args.new_member_fqdn,
            skip_puppet_bootstrap=args.skip_puppet_bootstrap,
            project=args.project,
            spicerack=self.spicerack,
        )


def _fix_apiserver_yaml(node: RemoteHosts, etcd_members: List[str]):
    members_urls = [f"https://{fqdn}:2379" for fqdn in etcd_members]
    new_etcd_members_arg = "--etcd-servers=" + ",".join(sorted(members_urls, key=natural_sort_key))
    apiserver_config_file = "/etc/kubernetes/manifests/kube-apiserver.yaml"
    apiserver_config = yaml.safe_load(next(node.run_sync(f"cat {apiserver_config_file}"))[1].message().decode())
    # we expect the container to be the first and only in the spec
    command_args = apiserver_config["spec"]["containers"][0]["command"]
    for index, arg in enumerate(command_args):
        if arg.startswith("--etcd-servers="):
            if arg == new_etcd_members_arg:
                LOGGER.info("Apiserver yaml file was already ok on %s", node)
                return

            command_args[index] = new_etcd_members_arg
            apiserver_config_str = yaml.dump(apiserver_config)
            simple_create_file(
                remote_path=apiserver_config_file,
                dst_node=node,
                contents=apiserver_config_str,
                use_root=True,
            )
            LOGGER.info("Fixed apiserver yaml file on %s.", node)
            return


def _add_node_to_kubeadm_configmap(k8s_control_node: RemoteHosts, new_etcd_member_fqdn: str) -> str:
    namespace = "kube-system"
    configmap = "kubeadm-config"
    kubeadm_config = yaml.safe_load(
        next(k8s_control_node.run_sync(f"kubectl --namespace={namespace} get configmap {configmap} -o yaml"))[1]
        .message()
        .decode()
    )
    # double yaml yep xd
    cluster_config = yaml.safe_load(kubeadm_config["data"]["ClusterConfiguration"])

    new_endpoint = f"https://{new_etcd_member_fqdn}:2379"
    if new_endpoint not in cluster_config["etcd"]["external"]["endpoints"]:
        LOGGER.info("Updating Kubeadm configmap %s/%s.", namespace, configmap)
        cluster_config["etcd"]["external"]["endpoints"].append(new_endpoint)
    else:
        LOGGER.info(
            "Kubeadm configmap %s/%s already contained %s, not updating.",
            namespace,
            configmap,
            new_endpoint,
        )
        return ""

    kubeadm_config["data"]["ClusterConfiguration"] = yaml.dump(cluster_config)
    kubeadm_config["metadata"] = {
        "name": configmap,
        "namespace": namespace,
    }
    kubeadm_config_str = yaml.dump(kubeadm_config)
    # avoid quoting/bash escaping issues
    kubeadm_config_base64 = base64.b64encode(kubeadm_config_str.encode("utf8"))
    return (
        next(
            k8s_control_node.run_sync(
                " | ".join(
                    [
                        f"echo '{kubeadm_config_base64.decode()}'",
                        "base64 --decode",
                        # this sudo is needed until we have proper support in spicerack
                        "sudo -i kubectl apply --filename=-",
                    ]
                )
            )
        )[1]
        .message()
        .decode()
    )


def _fix_kubeadm(
    remote: Remote,
    k8s_control_members: List[str],
    new_etcd_member_fqdn: str,
    existing_etcd_members: List[str],
):
    for k8s_control_node_fqdn in k8s_control_members:
        _fix_apiserver_yaml(
            node=remote.query(f"D{{{k8s_control_node_fqdn}}}", use_sudo=True),
            etcd_members=existing_etcd_members + [new_etcd_member_fqdn],
        )
        # give time for etcd to stabilize
        time.sleep(10)

    # just pick the first, any should do
    k8s_control_node = remote.query(f"D{{{k8s_control_members[0]}}}", use_sudo=True)
    _add_node_to_kubeadm_configmap(
        k8s_control_node=k8s_control_node,
        new_etcd_member_fqdn=new_etcd_member_fqdn,
    )


class AddNodeToClusterRunner(CookbookRunnerBase):
    """Runner for AddNodeToCluster"""

    def __init__(
        self,
        etcd_prefix: str,
        new_member_fqdn: bool,
        skip_puppet_bootstrap: bool,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.etcd_prefix = etcd_prefix
        self.new_member_fqdn = new_member_fqdn
        self.skip_puppet_bootstrap = skip_puppet_bootstrap
        self.project = project
        self.spicerack = spicerack

    def run(self) -> Optional[int]:
        """Main entry point"""
        remote = self.spicerack.remote()
        etcd_prefix = self.etcd_prefix if self.etcd_prefix is not None else f"{self.project}-k8s-etcd"

        if not self.skip_puppet_bootstrap:
            LOGGER.info("Bootstraping puppet on the new member. Note that etcd will not be able to start yet.")
            refresh_puppet_certs_cookbook = RefreshPuppetCerts(spicerack=self.spicerack)
            refresh_puppet_certs_cookbook.get_runner(
                args=refresh_puppet_certs_cookbook.argument_parser().parse_args(["--fqdn", self.new_member_fqdn]),
            ).run()
        else:
            LOGGER.info("Skipping the puppet bootstrapping (--skip-puppet-bootstrap)")

        LOGGER.info("Adding node to the hiera configuration")
        add_node_to_hiera_cookbook = AddNodeToHiera(spicerack=self.spicerack)
        hiera_data = add_node_to_hiera_cookbook.get_runner(
            args=add_node_to_hiera_cookbook.argument_parser().parse_args(
                [
                    "--project",
                    self.project,
                    "--prefix",
                    etcd_prefix,
                    "--fqdn-to-add",
                    self.new_member_fqdn,
                ]
            ),
        ).run()
        LOGGER.info("Give some time for caches to flush")
        time.sleep(30)

        etcd_members = list(sorted(hiera_data["profile::toolforge::k8s::etcd_nodes"], key=natural_sort_key))
        if self.skip_puppet_bootstrap:
            LOGGER.info("Skipping the refresh of all the ssl certs in the cluster " "(--skip-puppet-bootstrap)")
        else:
            LOGGER.info("Refreshing certs on all etcd members (to get the new alt-names)")
            self._do_puppet_bootstrap(
                new_etcd_member_fqdn=self.new_member_fqdn,
                etcd_members=etcd_members,
            )

        existing_etcd_member_fqdn = etcd_members[0]
        # this might happen when the new member is number 10, as the sorting is
        # alphadecimal, so 10 goes before 1
        if existing_etcd_member_fqdn == self.new_member_fqdn:
            existing_etcd_member_fqdn = etcd_members[1]

        existing_etcd_member_node = remote.query(f"D{{{existing_etcd_member_fqdn}}}", use_sudo=True)
        self.spicerack.etcdctl(remote_host=existing_etcd_member_node).ensure_node_exists(
            new_member_fqdn=self.new_member_fqdn,
        )

        LOGGER.info(
            "Rerunning puppet on the new host to force etcd to start and join the cluster now that all the members "
            "have the correct configs."
        )
        new_etcd_member_puppet = self.spicerack.puppet(remote.query(f"D{{{self.new_member_fqdn}}}", use_sudo=True))
        new_etcd_member_puppet.run()

        LOGGER.info("Updating the kubernetes configs to let the control nodes know about the new etcd member.")
        k8s_control_members = list(sorted(hiera_data["profile::toolforge::k8s::control_nodes"], key=natural_sort_key))
        _fix_kubeadm(
            remote=remote,
            k8s_control_members=k8s_control_members,
            new_etcd_member_fqdn=self.new_member_fqdn,
            existing_etcd_members=etcd_members,
        )

    def _do_puppet_bootstrap(self, new_etcd_member_fqdn: str, etcd_members: List[str]) -> None:
        # done one by one to avoid taking the cluster down
        for etcd_member in etcd_members:
            if etcd_member == new_etcd_member_fqdn:
                continue

            refresh_puppet_certs_cookbook = RefreshPuppetCerts(spicerack=self.spicerack)
            refresh_puppet_certs_cookbook.get_runner(
                args=refresh_puppet_certs_cookbook.argument_parser().parse_args(["--fqdn", etcd_member]),
            ).run()
            # give time for etcd to stabilize
            time.sleep(10)
