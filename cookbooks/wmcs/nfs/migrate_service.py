"""WMCS Toolforge - Migrate a given NFS volume from one host to another

Usage example:
    cookbook wmcs.nfs.migrate_service \
        --from-id <old server id> \
        --to-id <new server id> \
        --project <project_id> \
        --force

the old and new hosts must already have been created using similar add_server
calls such that they have the same puppet/hiera config.
"""
import argparse
import json
import logging
from typing import Union

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetHosts

from cookbooks.wmcs import OutputFormat, run_one_as_dict, run_one_raw
from cookbooks.wmcs.lib.openstack import OpenstackAPI

LOGGER = logging.getLogger(__name__)

OpenstackID = str
OpenstackName = str
OpenstackIdentifier = Union[OpenstackID, OpenstackName]


def _quote(mystr: str) -> str:
    """Wraps the given string in single quotes."""
    return f"'{mystr}'"


class NFSServiceMigrateVolume(CookbookBase):
    """WMCS Toolforge cookbook to move nfs service from one VM to another

    Both new and old servers must have been prepared using the nfs/add_server
    cookbook.
    """

    title = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(prog=__name__, description=__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument("--from-host-id", required=True, help="old service host ID")
        parser.add_argument("--to-host-id", required=True, help="new service host ID")
        parser.add_argument("--project", required=True, help="openstack project id containing both hosts")
        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "If set, do not try to stop existing services and unmount volume from old host. "
                "Useful when the oldhost is down."
            ),
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return NFSServiceMigrateVolumeRunner(
            project=args.project,
            from_id=args.from_host_id,
            to_id=args.to_host_id,
            force=args.force,
            spicerack=self.spicerack,
        )


class NFSServiceMigrateVolumeRunner(CookbookRunnerBase):
    """Runner for NFSServiceMigrateVolume"""

    def __init__(self, project, from_id: OpenstackID, to_id: OpenstackID, force: bool, spicerack: Spicerack):
        """Init"""
        self.from_id = from_id
        self.to_id = to_id
        self.project = project
        self.force = force
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(
            remote=self.spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=self.project
        )
        self.from_server = self.openstack_api.server_from_id(self.from_id)
        self.to_server = self.openstack_api.server_from_id(self.to_id)

        self.from_name = self.from_server["name"]
        self.to_name = self.to_server["name"]

        self.from_fqdn = f"{self.from_name}.{self.project}.eqiad1.wikimedia.cloud"
        self.to_fqdn = f"{self.to_name}.{self.project}.eqiad1.wikimedia.cloud"

    def run(self) -> None:  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
        """Main entry point"""
        if not self.from_server["volumes_attached"] and self.force:
            LOGGER.warning("Source server has no volume attached, checking if target already has an attachment")
            volume_id = self.to_server["volumes_attached"][0]["id"]
        else:
            volume_id = self.from_server["volumes_attached"][0]["id"]
        volume = self.openstack_api.volume_from_id(volume_id)
        volume_name = volume["name"]

        from_node = self.spicerack.remote().query(f"D{{{self.from_fqdn}}}", use_sudo=True)
        to_node = self.spicerack.remote().query(f"D{{{self.to_fqdn}}}", use_sudo=True)

        # Verify that puppet/hiera config agrees between the two hosts
        control_node = self.spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True)

        response = run_one_as_dict(
            node=control_node,
            command=["wmcs-enc-cli", "--openstack-project", self.project, "get_node_consolidated_info", self.from_fqdn],
            try_format=OutputFormat.YAML,
            is_safe=True,
        )

        from_hiera = response["hiera"]
        from_roles = response["roles"]

        if "role::wmcs::nfs::standalone" not in from_roles:
            raise Exception(
                f"Server {self.from_fqdn} does not use role::wmcs::nfs::standalone "
                f"This cookbook only works on that role. Roles are {from_roles}"
            )

        if (
            "profile::wmcs::nfs::standalone::volumes" not in from_hiera
            or len(from_hiera["profile::wmcs::nfs::standalone::volumes"]) != 1
        ):
            raise Exception(
                f"Server {self.from_fqdn} must have exactly one value set for profile::wmcs::nfs::standalone::volumes."
            )

        mount_name = from_hiera["profile::wmcs::nfs::standalone::volumes"][0]

        response = run_one_as_dict(
            node=control_node,
            command=["wmcs-enc-cli", "--openstack-project", self.project, "get_node_consolidated_info", self.to_fqdn],
            try_format=OutputFormat.YAML,
            is_safe=True,
        )
        to_hiera = response["hiera"]
        to_roles = response["roles"]

        if "role::wmcs::nfs::standalone" not in to_roles:
            raise Exception(
                f"Server {self.to_fqdn} does not use role::wmcs::nfs::standalone "
                f"This cookbook only works on that role. Roles are {to_roles}"
            )

        if (
            "profile::wmcs::nfs::standalone::volumes" not in to_hiera
            or len(to_hiera["profile::wmcs::nfs::standalone::volumes"]) != 1
            or to_hiera["profile::wmcs::nfs::standalone::volumes"][0] != mount_name
        ):
            raise Exception(
                f"Server {self.to_fqdn} must have profile::wmcs::nfs::standalone::volumes: ['{mount_name}']"
            )

        if (
            "profile::wmcs::nfs::standalone::cinder_attached" in to_hiera
            and to_hiera["profile::wmcs::nfs::standalone::cinder_attached"]
            and not self.force
        ):
            raise Exception(
                f"Server {self.to_fqdn} already seems to have a volume attached "
                "(profile::wmcs::nfs::standalone::cinder_attached=True)"
            )

        # locate the service IP
        service_fqdn = f"{volume_name}.svc.{self.project}.eqiad1.wikimedia.cloud"
        service_ip = run_one_raw(node=to_node, command=["dig", "+short", service_fqdn], last_line_only=True).strip()
        if not service_ip:
            raise Exception(f"Unable to resolve service ip for service name {service_fqdn}")
        service_ip_port = self.openstack_api.port_get(service_ip)[0]

        if service_ip_port["Name"] != mount_name:
            raise Exception(f"service ip name mismatch. Expected {mount_name}, found {service_ip_port['Name']}")

        to_ip = run_one_raw(node=to_node, command=["dig", "+short", self.to_fqdn], last_line_only=True).strip()
        to_port = self.openstack_api.port_get(to_ip)
        from_ip = run_one_raw(node=to_node, command=["dig", "+short", self.from_fqdn], last_line_only=True).strip()
        from_port = self.openstack_api.port_get(from_ip)

        # See if wmcs-prepare-cinder-volume has already been run on the target host
        volume_path = f"/srv/{mount_name}"
        volume_prepared = False

        fstab_content = run_one_raw(node=to_node, command=["cat", "/etc/fstab"])

        if volume_path in fstab_content:
            volume_prepared = True

        # That's all our checks. No start actually doing things.

        # Disable puppet to avoid races
        to_puppet = PuppetHosts(to_node)
        from_puppet = PuppetHosts(from_node)

        reason = self.spicerack.admin_reason(f"migrating nfs service from {self.from_fqdn} to {self.to_fqdn}")
        to_puppet.disable(reason)

        if not self.force:
            from_puppet.disable(reason)
            run_one_raw(node=from_node, command=["systemctl", "stop", "nfs-server.service"])
            run_one_raw(node=from_node, command=["umount", volume_path])

        try:
            self.openstack_api.volume_detach(self.from_id, volume_id)
            self.openstack_api.volume_attach(self.to_id, volume_id)
        except Exception as error:  # pylint: disable=broad-except
            if not self.force:
                LOGGER.warning("Ignoring exception due to --force: %s", error)
                raise error

        if volume_prepared:
            # Don't fail if it's already mounted.
            to_node.run_sync(f"mount {volume_path}; true")
        else:
            run_one_raw(
                node=to_node,
                command=[
                    "wmcs-prepare-cinder-volume",
                    "--device",
                    "sdb",
                    "--options",
                    "'rw,nofail,x-systemd.device-timeout=2s,noatime,data=ordered'",
                    "--mountpoint",
                    volume_path,
                    "--force",
                ],
            )

        # Tell puppet that cinder is detached on the old host and attached on the new one
        from_hiera["profile::wmcs::nfs::standalone::cinder_attached"] = False
        from_hiera_str = json.dumps(from_hiera)

        run_one_raw(
            node=control_node,
            command=[
                "wmcs-enc-cli",
                "--openstack-project",
                self.project,
                "set_prefix_hiera",
                self.from_fqdn,
                _quote(from_hiera_str),
            ],
            try_format=OutputFormat.YAML,
            is_safe=True,
        )

        to_hiera["profile::wmcs::nfs::standalone::cinder_attached"] = True
        to_hiera_str = json.dumps(to_hiera)
        run_one_raw(
            node=control_node,
            command=[
                "wmcs-enc-cli",
                "--openstack-project",
                self.project,
                "set_prefix_hiera",
                self.to_fqdn,
                _quote(to_hiera_str),
            ],
            try_format=OutputFormat.YAML,
            is_safe=True,
        )

        # Move the service ip
        try:
            self.openstack_api.detach_service_ip(service_ip, from_port[0]["MAC Address"], from_port[0]["ID"])
            self.openstack_api.attach_service_ip(service_ip, to_port[0]["ID"])
        except Exception as error:  # pylint: disable=broad-except
            if not self.force:
                LOGGER.warning("Ignoring exception due to --force: %s", error)
                raise error

        # Apply all pending puppet changes
        if not self.force:
            from_puppet.enable(reason)
            from_puppet.run()

        to_puppet.enable(reason)
        to_puppet.run()
        run_one_raw(node=to_node, command=["systemctl", "start", "nfs-server.service"])
