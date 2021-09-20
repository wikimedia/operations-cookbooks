"""WMCS Toolforge - Depool and delete the given etcd node from a toolforge installation

Usage example:
    cookbook wmcs.toolforge.etcd.depool_and_remove_node \
        --project toolsbeta \
        --node-fqdn toolsbeta-test-etcd-8.toolsbeta.eqiad1.wikimedia.cloud \
        --etcd-prefix toolsbeta-test-etcd

"""
import argparse
import base64
import logging
import time
from typing import List, Optional

import yaml
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import Remote, RemoteHosts

from cookbooks.wmcs import OpenstackAPI, natural_sort_key, simple_create_file
from cookbooks.wmcs.toolforge.etcd.remove_node_from_hiera import RemoveNodeFromHiera
from cookbooks.wmcs.vps.refresh_puppet_certs import RefreshPuppetCerts
from cookbooks.wmcs.vps.remove_instance import RemoveInstance

LOGGER = logging.getLogger(__name__)


class ToolforgeDepoolAndRemoveNode(CookbookBase):
    """WMCS Toolforge cookbook to remove and delete an existing etcd node"""

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
            "--fqdn-to-remove",
            required=False,
            help="FQDN of the node to remove, if none passed will remove the intance with the lower index.",
        )
        parser.add_argument(
            "--etcd-prefix",
            required=False,
            default=None,
            help=("Prefix for the k8s etcd nodes, default is <project>-k8s-etcd"),
        )
        parser.add_argument(
            "--skip-etcd-certs-refresh",
            action="store_true",
            help=(
                "Skip all the etcd certificate refreshing, useful if you "
                "already did it and you are rerunning, or if you did it "
                "manually"
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return ToolforgeDepoolAndRemoveNodeRunner(
            etcd_prefix=args.etcd_prefix,
            fqdn_to_remove=args.fqdn_to_remove,
            skip_etcd_certs_refresh=args.skip_etcd_certs_refresh,
            project=args.project,
            spicerack=self.spicerack,
        )


def _fix_apiserver_yaml(node: RemoteHosts, etcd_members: List[str]):
    members_urls = [f"https://{fqdn}:2379" for fqdn in etcd_members]
    new_etcd_members_arg = "--etcd-servers=" + ",".join(sorted(members_urls, key=natural_sort_key))
    apiserver_config_file = "/etc/kubernetes/manifests/kube-apiserver.yaml"
    apiserver_config = yaml.safe_load(next(node.run_sync(f"cat '{apiserver_config_file}'"))[1].message().decode())
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


def _remove_node_from_kubeadm_configmap(k8s_control_node: RemoteHosts, etcd_fqdn_to_remove: str) -> str:
    namespace = "kube-system"
    configmap = "kubeadm-config"
    kubeadm_config = yaml.safe_load(
        next(k8s_control_node.run_sync(f"kubectl --namespace='{namespace}' get configmap {configmap} -o yaml"))[1]
        .message()
        .decode()
    )
    # double yaml yep xd
    cluster_config = yaml.safe_load(kubeadm_config["data"]["ClusterConfiguration"])

    old_endpoint = f"https://{etcd_fqdn_to_remove}:2379"
    if old_endpoint in cluster_config["etcd"]["external"]["endpoints"]:
        cluster_config["etcd"]["external"]["endpoints"].pop(
            cluster_config["etcd"]["external"]["endpoints"].index(old_endpoint)
        )
    else:
        LOGGER.info("Kubeadm configmap %s/%s was already ok.", namespace, configmap)
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
                f"""echo '{kubeadm_config_base64.decode()}' |
                base64 --decode |
                sudo -i kubectl apply --filename=-
                """
            )
        )[1]
        .message()
        .decode()
    )


def _fix_kubeadm(
    remote: Remote,
    k8s_control_members: List[str],
    etcd_fqdn_to_remove: str,
    etcd_members: List[str],
):
    for k8s_control_node_fqdn in k8s_control_members:
        _fix_apiserver_yaml(
            node=remote.query(f"D{{{k8s_control_node_fqdn}}}", use_sudo=True),
            etcd_members=etcd_members,
        )
        # give time for etcd to stabilize
        time.sleep(10)

    # just pick the first, any should do
    k8s_control_node = remote.query(f"D{{{k8s_control_members[0]}}}", use_sudo=True)
    _remove_node_from_kubeadm_configmap(
        k8s_control_node=k8s_control_node,
        etcd_fqdn_to_remove=etcd_fqdn_to_remove,
    )


class ToolforgeDepoolAndRemoveNodeRunner(CookbookRunnerBase):
    """Runner for ToolforgeDepoolAndRemoveNode"""

    def __init__(
        self,
        etcd_prefix: str,
        fqdn_to_remove: str,
        skip_etcd_certs_refresh: bool,
        project: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.etcd_prefix = etcd_prefix
        self.fqdn_to_remove = fqdn_to_remove
        self.skip_etcd_certs_refresh = skip_etcd_certs_refresh
        self.project = project
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=self.project
        )

    def run(self) -> Optional[int]:
        """Main entry point"""
        remote = self.spicerack.remote()
        etcd_prefix = self.etcd_prefix if self.etcd_prefix is not None else f"{self.project}-k8s-etcd"
        if not self.fqdn_to_remove:
            all_project_servers = self.openstack_api.server_list()
            prefix_members = list(
                sorted(
                    (server for server in all_project_servers if server.get("Name", "noname").startswith(etcd_prefix)),
                    key=lambda server: natural_sort_key(server.get("Name", "noname-0")),
                )
            )
            if not prefix_members:
                raise Exception(f"No servers in project {self.project} with prefix {etcd_prefix}, nothing to remove.")

            # TODO: find a way to not hardcode the domain
            fqdn_to_remove = f"{prefix_members[0]['Name']}.{self.project}.eqiad1.wikimedia.cloud"

        else:
            fqdn_to_remove = self.fqdn_to_remove

        LOGGER.info("Removing etcd member %s...", fqdn_to_remove)
        remove_node_from_hiera_cookbook = RemoveNodeFromHiera(spicerack=self.spicerack)
        hiera_data = remove_node_from_hiera_cookbook.get_runner(
            args=remove_node_from_hiera_cookbook.argument_parser().parse_args(
                [
                    "--project",
                    self.project,
                    "--prefix",
                    etcd_prefix,
                    "--fqdn-to-remove",
                    fqdn_to_remove,
                ]
            ),
        ).run()
        # Give some time for caches to flush
        time.sleep(30)

        etcd_members = list(sorted(hiera_data["profile::toolforge::k8s::etcd_nodes"], key=natural_sort_key))
        other_etcd_member = etcd_members[0]
        other_etcd_node = remote.query(f"D{{{other_etcd_member}}}", use_sudo=True)
        self.spicerack.etcdctl(remote_host=other_etcd_node).ensure_node_does_not_exist(member_fqdn=fqdn_to_remove)

        if self.skip_etcd_certs_refresh:
            LOGGER.info("Skipping the refresh of all the ssl certs in the cluster (--skip-etcd-certs-refresh)")
        else:
            self._refresh_etcd_certs(etcd_members=etcd_members)

        k8s_control_members = list(sorted(hiera_data["profile::toolforge::k8s::control_nodes"], key=natural_sort_key))
        _fix_kubeadm(
            remote=remote,
            k8s_control_members=k8s_control_members,
            etcd_fqdn_to_remove=fqdn_to_remove,
            etcd_members=etcd_members,
        )

        remove_instance_cookbook = RemoveInstance(spicerack=self.spicerack)
        remove_instance_cookbook.get_runner(
            args=remove_instance_cookbook.argument_parser().parse_args(
                [
                    "--project",
                    self.project,
                    "--server-name",
                    fqdn_to_remove.split(".", 1)[0],
                ],
            ),
        ).run()

    def _refresh_etcd_certs(self, etcd_members: List[str]) -> None:
        # refresh the puppet certs with the new alt-name, we use puppet certs
        # for etcd too.
        # TODO: might be interesting to have this as it's own cookbook
        # eventually
        for etcd_member in etcd_members:
            # done one by one to avoid taking the cluster down
            refresh_puppet_certs_cookbook = RefreshPuppetCerts(spicerack=self.spicerack)
            refresh_puppet_certs_cookbook.get_runner(
                args=refresh_puppet_certs_cookbook.argument_parser().parse_args(["--fqdn", etcd_member]),
            ).run()
            # give time for etcd to stabilize
            time.sleep(10)
