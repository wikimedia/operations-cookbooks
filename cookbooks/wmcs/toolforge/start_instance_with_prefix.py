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

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError
from wmflib.decorators import retry

from cookbooks.wmcs import OpenstackAPI, natural_sort_key

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
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(), control_node_fqdn="cloudcontrol1003.wikimedia.org", project=project
        )
        self.project = project
        self.prefix = prefix
        self.flavor = flavor
        self.network = network
        self.image = image
        self.server_group = server_group or self.prefix
        self.spicerack = spicerack
        self.security_group = security_group or f"{self.project}-k8s-full-connectivity"

    def run(self) -> Optional[int]:  # pylint: disable-msg=too-many-locals
        """Main entry point"""
        self.openstack_api.security_group_ensure(
            security_group=self.security_group,
            description="This group provides full access from its members to its members.",
        )
        self.openstack_api.server_group_ensure(server_group=self.server_group)

        all_project_servers = self.openstack_api.server_list()
        other_prefix_members = list(
            sorted(
                (server for server in all_project_servers if server.get("Name", "noname").startswith(self.prefix)),
                key=lambda server: natural_sort_key(server.get("Name", "noname-0")),
            )
        )
        if not other_prefix_members:
            missing_params = [
                param_name for param_name in ["flavor", "image", "network"] if not getattr(self, param_name)
            ]
            if missing_params:
                message = (
                    "As there's no other prefix members, I can't add a new member without explicitly specifying the "
                    f"missing {', '.join(missing_params)} options."
                )
                LOGGER.error(message)
                raise Exception(message)

            last_prefix_member_id = 0

        last_prefix_member_name = other_prefix_members[-1]["Name"]
        last_prefix_member_id = int(last_prefix_member_name.rsplit("-", 1)[-1])

        new_prefix_member_name = f"{self.prefix}-{last_prefix_member_id + 1}"
        maybe_security_group = self.openstack_api.security_group_by_name(name=self.security_group)
        if maybe_security_group is None:
            raise Exception(
                f"Unable to find a '{self.security_group}' security group for project {self.project}, though it "
                "should have been created before if not there."
            )

        security_group_id: str = maybe_security_group["ID"]

        maybe_default_security_group = self.openstack_api.security_group_by_name(name="default")
        if maybe_default_security_group is None:
            raise Exception(f"Unable to find a default security group for project {self.project}")

        default_security_group_id: str = maybe_default_security_group["ID"]

        maybe_server_group = self.openstack_api.server_group_by_name(name=self.server_group)
        if maybe_server_group is None:
            raise Exception(
                f"Unable to find a server group with name {self.server_group} for project {self.project}, though it "
                "should have been created before if not there."
            )

        server_group_id: str = maybe_server_group["ID"]

        self.openstack_api.server_create(
            flavor=self.flavor or other_prefix_members[-1]["Flavor"],
            security_group_ids=[default_security_group_id, security_group_id],
            server_group_id=server_group_id,
            image=self.image or other_prefix_members[-1]["Image"],
            network=self.network or other_prefix_members[-1]["Networks"].split("=", 1)[0],
            name=new_prefix_member_name,
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
