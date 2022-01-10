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
import logging
from typing import Optional, List
import json
import yaml

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.toolforge.start_instance_with_prefix import StartInstanceWithPrefix
from cookbooks.wmcs.toolforge.start_instance_with_prefix import add_instance_creation_options
from cookbooks.wmcs.toolforge.start_instance_with_prefix import with_instance_creation_options
from cookbooks.wmcs.toolforge.start_instance_with_prefix import InstanceCreationOpts
from cookbooks.wmcs import OpenstackAPI

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
            "--create-storage-volume-size",
            type=int,
            required=False,
            default=None,
            help="Size for created storage volume. If unset, no volume will be created; "
            "an existing volume can be attached later.",
        )
        add_instance_creation_options(parser)
        parser.add_argument("volumes", nargs="+", help=("nfs volumes to be provided and managed by this server"))

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_instance_creation_options(args, NFSAddServerRunner)(
            prefix=args.prefix,
            project=args.project,
            volumes=args.volumes,
            create_storage_volume_size=args.create_storage_volume_size,
            spicerack=self.spicerack,
        )


class NFSAddServerRunner(CookbookRunnerBase):
    """Runner for NFSAddServer"""

    def __init__(
        self,
        prefix: str,
        project: str,
        volumes: List[str],
        create_storage_volume_size: int,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
    ):
        """Init"""
        self.create_storage_volume_size = create_storage_volume_size
        self.volumes = volumes
        self.project = project
        self.spicerack = spicerack
        self.prefix = prefix
        self.instance_creation_opts = instance_creation_opts

    def run(self) -> Optional[int]:
        """Main entry point"""
        prefix = self.prefix if self.prefix is not None else f"{self.volumes[0]}"

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

        start_instance_cookbook = StartInstanceWithPrefix(spicerack=self.spicerack)
        new_server = start_instance_cookbook.get_runner(
            args=start_instance_cookbook.argument_parser().parse_args(start_args)
        ).run()
        new_node = self.spicerack.remote().query(f"D{{{new_server.server_fqdn}}}", use_sudo=True)

        if self.create_storage_volume_size > 0:
            openstack_api = OpenstackAPI(
                remote=self.spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=self.project
            )
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

        # Add nfs volumes
        current_hiera["profile::wcms::nfs::standalone::volumes"] = self.volumes
        if self.create_storage_volume_size > 0:
            current_hiera["profile::wcms::nfs::standalone::cinder_attached"] = True
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
                ("wmcs-prepare-cinder-volume --device sdb --options "
                 "'rw,nofail,x-systemd.device-timeout=2s,noatime,data=ordered' "
                 f"--mountpoint '/srv/{self.volumes[0]}' --force")
            )
