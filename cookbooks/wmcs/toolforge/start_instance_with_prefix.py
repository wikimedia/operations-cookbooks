"""WMCS Toolforge - Add a new instance to the given prefix.

It will make sure to use the same flavor, network, groups and increment the
index of the existing instance with the same prefix unless you pass a specific
one.
NOTE: it requires for an instance to be already there (TODO: allow creating
a without previous instances).

Usage example:
    cookbook wmcs.toolforge.start_instance_with_prefix \
        --project toolsbeta \
        --prefix toolsbeta-k8s-test-etcd \
        --security-group toolsbeta-k8s-full-connectivity

"""
# pylint: disable=unsubscriptable-object,too-many-arguments
import argparse
import logging
from datetime import timedelta
from typing import Optional

from wmflib.decorators import retry
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError

from cookbooks.wmcs import get_run_os

LOGGER = logging.getLogger(__name__)


class StartInstanceWithPrefix(CookbookBase):
    """WMCS Toolforge cookbook to start a new instance following a prefix."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=self.__doc__,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        parser.add_argument(
            "--prefix",
            required=False,
            default=None,
            help="Prefix for the instance (ex. toolsbeta-test-k8s-etcd).",
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
        parser.add_argument(
            "--network",
            required=False,
            default=None,
            help=(
                "Network for the new instance (will use the same as the latest existing one by default, ex. "
                "lan-flat-cloudinstances2b, ex. a69bdfad-d7d2-4cfa-8231-3d6d3e0074c9)"
            ),
        )
        parser.add_argument(
            "--security-group",
            required=False,
            default=None,
            help=(
                "Extra security group to put the instance in (will alway add the 'default' security group, and then "
                "this one, '<project>-k8s-full-connectivity' by default). If it does not exist it will be created "
                "allowing all traffic between instances of the group (ex. )."
            ),
        )
        parser.add_argument(
            "--server-group",
            required=False,

            help=(
                "Server group to start the instance in. If it does not exist, it well create it with anti-affinity "
                "policy, will use the same as '--prefix' by default (ex. toolsbeta-test-k8s-etcd)."
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return StartInstanceWithPrefixRunner(
            security_group=args.security_group,
            server_group=args.server_group,
            project=args.project,
            prefix=args.prefix,
            flavor=args.flavor,
            image=args.image,
            network=args.network,
            spicerack=self.spicerack,
        )


class StartInstanceWithPrefixRunner(CookbookRunnerBase):
    """Runner for StartInstanceWithPrefix"""

    def __init__(
        self,
        project: str,
        prefix: str,
        spicerack: Spicerack,
        server_group: Optional[str] = None,
        security_group: Optional[str] = None,
        flavor: Optional[str] = None,
        image: Optional[str] = None,
        network: Optional[str] = None,
    ):
        """Init"""
        self.run_os = get_run_os(
            control_node=spicerack.remote().query("D{cloudcontrol1003.wikimedia.org}", use_sudo=True),
            project=project,
        )

        self.project = project
        self.prefix = prefix
        self.flavor = flavor
        self.network = network
        self.image = image
        self.server_group = server_group or self.prefix
        self.spicerack = spicerack
        self.security_group = security_group or f"{self.project}-k8s-full-connectivity"

    def _ensure_security_group(self, security_group: str) -> None:
        existing_security_groups = self.run_os("security", "group", "list", is_safe=True)
        if not any(
            True
            for existing_security_group in existing_security_groups
            if existing_security_group.get("Name", "") == security_group
        ):
            LOGGER.info("Creating security group %s...", security_group)
            self.run_os(
                "security",
                "group",
                "create",
                security_group,
                "--description",
                "'This group provides full access from its members to its members.'",
            )
            self.run_os(
                "security",
                "group",
                "rule",
                "create",
                "--egress",
                "--remote-group",
                security_group,
                "--protocol",
                "any",
                security_group,
            )
            self.run_os(
                "security",
                "group",
                "rule",
                "create",
                "--ingress",
                "--remote-group",
                security_group,
                "--protocol",
                "any",
                security_group,
            )
        else:
            LOGGER.info(
                "Security group %s already exists, not creating.",
                security_group,
            )

    def _ensure_server_group(self, server_group: str) -> None:
        # it seems that on cli the project flag shows nothing :/ so we have to
        # list all of them.
        existing_server_groups = self.run_os("server", "group", "list", is_safe=True)
        if not any(
            True
            for existing_server_group in existing_server_groups
            if existing_server_group.get("Name", "") == server_group
        ):
            LOGGER.info("Creating server group %s...", server_group)
            self.run_os(
                "server",
                "group",
                "create",
                "--policy",
                "anti-affinity",
                server_group,
            )
        else:
            LOGGER.info("Server group %s already exists, not creating.", server_group)

    def run(self) -> Optional[int]:  # pylint: disable-msg=too-many-locals
        """Main entry point"""
        self._ensure_security_group(security_group=self.security_group)
        self._ensure_server_group(server_group=self.server_group)

        all_project_servers = self.run_os("server", "list", is_safe=True)
        other_prefix_members = list(
            sorted(
                (server for server in all_project_servers if server.get("Name", "noname").startswith(self.prefix)),
                key=lambda server: server.get("Name", "noname"),
            )
        )
        if not other_prefix_members:
            missing_params = [
                param_name for param_name in ['flavor', 'image', 'network'] if not getattr(self, param_name)
            ]
            if missing_params:
                message = (
                    "As there's no other prefix members, I can't add a new member without explicitly specifying the "
                    f"missing {', '.join(missing_params)} options."
                )
                LOGGER.error(message)
                raise Exception(message)

            last_prefix_member_id = 0
        else:
            last_prefix_member_name = other_prefix_members[-1]["Name"]
            last_prefix_member_id = int(last_prefix_member_name.rsplit("-", 1)[-1])

        new_prefix_member_name = f"{self.prefix}-{last_prefix_member_id + 1}"
        # refresh the list of groups so we get the uuid of the new one
        existing_server_groups = self.run_os("server", "group", "list", is_safe=True)
        server_group_info = next(
            existing_server_group
            for existing_server_group in existing_server_groups
            if existing_server_group.get("Name", "") == self.server_group
        )

        # get the ids of the security groups, as names might be repeated
        existing_security_groups = self.run_os("security", "group", "list", is_safe=True)
        default_security_group_id = None
        security_group_id = None
        for security_group in existing_security_groups:
            if security_group["Project"] == self.project:
                if security_group["Name"] == "default":
                    default_security_group_id = security_group["ID"]
                elif security_group["Name"] == self.security_group:
                    security_group_id = security_group["ID"]

        if default_security_group_id is None:
            raise Exception(f"Unable to find a default security group for project {self.project}")

        if security_group_id is None:
            raise Exception(f"Unable to find a '{self.security_group}' security group for project {self.project}")

        self.run_os(
            "server",
            "create",
            "--flavor",
            self.flavor or other_prefix_members[-1]["Flavor"],
            "--security-group",
            f'"{default_security_group_id}"',
            "--security-group",
            f'"{security_group_id}"',
            "--image",
            self.image or other_prefix_members[-1]["Image"],
            "--network",
            self.network or other_prefix_members[-1]["Networks"].split("=", 1)[0],
            "--hint",
            f'group={server_group_info["ID"]}',
            "--wait",
            new_prefix_member_name,
        )

        new_instance_fqdn = f"{new_prefix_member_name}.{self.project}.eqiad1.wikimedia.cloud"
        new_prefix_node = self.spicerack.remote().query(f"D{{{new_instance_fqdn}}}", use_sudo=True)

        @retry(
            tries=15,
            delay=timedelta(minutes=1),
            backoff_mode="constant",
            exceptions=(RemoteExecutionError,),
        )
        def try_to_reach_the_new_instance():
            new_prefix_node.run_sync("hostname")

        try_to_reach_the_new_instance()

        return new_instance_fqdn
