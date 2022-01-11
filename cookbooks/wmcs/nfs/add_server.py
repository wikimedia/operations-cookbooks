"""WMCS Toolforge - Add a new nfs server on a VM

Usage example:
    cookbook wmcs.nfs.add_server \
        --project cloudinfra-nfs \
        --create-storage-volume-size 200 \
        --prefix toolsbeta \
        toolsbeta-home toolsbeta-project

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import json
import logging
from typing import Optional

import yaml
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs import OpenstackAPI, run_one
from cookbooks.wmcs.vps.create_instance_with_prefix import (
    CreateInstanceWithPrefix,
    InstanceCreationOpts,
    add_instance_creation_options,
    with_instance_creation_options,
)

LOGGER = logging.getLogger(__name__)


class NFSAddServer(CookbookBase):
    """WMCS Toolforge cookbook to add a new nfs server"""

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__, description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
        )
        parser.add_argument(
            "--project", required=False, default="cloudinfra-nfs", help="Openstack project to contain the new server"
        )
        parser.add_argument(
            "--service-ip",
            action="store_true",
            help="If set, a service IP and fqdn will be created and attached to the new host.",
        )
        parser.add_argument(
            "--create-storage-volume-size",
            type=int,
            required=False,
            default=0,
            help="Size for created storage volume. If unset, no volume will be created; "
            "an existing volume can be attached later.",
        )
        add_instance_creation_options(parser)
        parser.add_argument("volume", help=("nfs volume to be provided and managed by this server"))

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_instance_creation_options(args, NFSAddServerRunner)(
            prefix=args.prefix,
            project=args.project,
            volume=args.volume,
            service_ip=args.service_ip,
            create_storage_volume_size=args.create_storage_volume_size,
            spicerack=self.spicerack,
        )


class NFSAddServerRunner(CookbookRunnerBase):
    """Runner for NFSAddServer"""

    def __init__(
        self,
        prefix: str,
        project: str,
        service_ip: bool,
        volume: str,
        create_storage_volume_size: int,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
    ):
        """Init"""
        self.create_storage_volume_size = create_storage_volume_size
        self.volume = volume
        self.project = project
        self.spicerack = spicerack
        self.prefix = prefix
        self.service_ip = service_ip
        self.instance_creation_opts = instance_creation_opts

    def run(self) -> Optional[int]:
        """Main entry point"""
        prefix = self.prefix if self.prefix is not None else f"{self.volume}"

        start_args = [
            "--project",
            self.project,
            "--prefix",
            prefix,
            "--security-group",
            f"{self.project}-k8s-full-connectivity",
            "--security-group",
            "nfs",
        ] + self.instance_creation_opts.to_cli_args()

        create_instance_cookbook = CreateInstanceWithPrefix(spicerack=self.spicerack)
        new_server = create_instance_cookbook.get_runner(
            args=create_instance_cookbook.argument_parser().parse_args(start_args)
        ).run()

        new_node = self.spicerack.remote().query(f"D{{{new_server.server_fqdn}}}", use_sudo=True)
        openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=self.project
        )

        if self.create_storage_volume_size > 0:
            new_volume = openstack_api.volume_create(self.prefix, self.create_storage_volume_size)

            openstack_api.volume_attach(new_server.server_id, new_volume)

        control_node = self.spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True)
        # Get current puppet config
        response = yaml.safe_load(
            next(
                control_node.run_sync(
                    (
                        f"wmcs-enc-cli --openstack-project {self.project} "
                        "get_node_consolidated_info {new_server.server_fqdn}"
                    ),
                    is_safe=True,
                )
            )[1]
            .message()
            .decode()
        )
        current_hiera = response["hiera"]
        current_roles = response["roles"]

        # Add nfs volume
        current_hiera["profile::wcms::nfs::standalone::volumes"] = [self.volume]
        if self.create_storage_volume_size > 0:
            current_hiera["profile::wcms::nfs::standalone::cinder_attached"] = True
        else:
            current_hiera["profile::wcms::nfs::standalone::cinder_attached"] = False
        current_hiera_str = json.dumps(current_hiera)
        response = yaml.safe_load(
            next(
                control_node.run_sync(
                    (
                        f"wmcs-enc-cli --openstack-project {self.project} "
                        f"set_prefix_hiera {new_server.server_fqdn} '{current_hiera_str}'"
                    )
                )
            )[1]
            .message()
            .decode()
        )

        # Add nfs server puppet role
        current_roles.append("role::wmcs::nfs::standalone")
        current_roles_str = json.dumps(current_roles)
        response = yaml.safe_load(
            next(
                control_node.run_sync(
                    (
                        f"wmcs-enc-cli --openstack-project {self.project} "
                        f"set_prefix_roles {new_server.server_fqdn} '{current_roles_str}'"
                    )
                )
            )[1]
            .message()
            .decode()
        )

        if self.create_storage_volume_size > 0:
            new_node.run_sync(
                (
                    "wmcs-prepare-cinder-volume --device sdb --options "
                    "'rw,nofail,x-systemd.device-timeout=2s,noatime,data=ordered' "
                    f"--mountpoint '/srv/{self.volume}' --force"
                )
            )

        if self.service_ip:
            new_server_ip = run_one(node=new_node, command=["dig", "+short", new_server.server_fqdn]).strip()

            host_port = openstack_api.port_get(new_server_ip)

            service_ip_response = openstack_api.create_service_ip(self.volume, self.instance_creation_opts.network)
            service_ip = service_ip_response["fixed_ips"][0]["ip_address"]

            logging.warning("The new service_ip is %s" % service_ip)
            openstack_api.attach_service_ip(service_ip, host_port[0]["ID"])

            zone_record = openstack_api.zone_get(f"svc.{self.project}.eqiad1.wikimedia.cloud.")
            openstack_api.recordset_create(
                zone_record[0]["id"], "A", f"{self.volume}.svc.{self.project}.eqiad1.wikimedia.cloud.", service_ip
            )

        # Apply all pending changes
        new_node.run_sync("/usr/local/sbin/run-puppet-agent")
