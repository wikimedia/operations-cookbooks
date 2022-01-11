"""WMCS Toolforge - Add a new instance to the given prefix.

It will make sure to use the same flavor, network, groups and increment the
index of the existing instance with the same prefix unless you pass a specific
one.
NOTE: it requires for an instance to be already there (TODO: allow creating
a without previous instances).

Usage example:
    cookbook wmcs.vps.create_instance_with_prefix \
        --project toolsbeta \
        --prefix toolsbeta-k8s-test-etcd \
        --security-group toolsbeta-k8s-full-connectivity

"""
# pylint: disable=too-many-arguments,no-value-for-parameter
import argparse
import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Callable, List
from functools import partial

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError
from wmflib.decorators import retry

from cookbooks.wmcs import OpenstackAPI, OpenstackServerGroupPolicy, natural_sort_key, OpenstackIdentifier, run_one

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CreateServerResponse:
    """Instance creation results."""

    server_id: OpenstackIdentifier
    server_fqdn: str


@dataclass(frozen=True)
class InstanceCreationOpts:
    """Instance creation options."""

    prefix: Optional[str] = None
    flavor: Optional[OpenstackIdentifier] = None
    image: Optional[OpenstackIdentifier] = None
    network: Optional[OpenstackIdentifier] = None
    security_group: Optional[OpenstackIdentifier] = None
    server_group: Optional[OpenstackIdentifier] = None
    server_group_policy: Optional[OpenstackServerGroupPolicy] = None

    def to_cli_args(self) -> List[str]:
        """Helper to unwrap the options for use with argument parsers."""
        args = []
        if self.prefix:
            args.extend(["--prefix", self.prefix])
        if self.flavor:
            args.extend(["--flavor", self.flavor])
        if self.image:
            args.extend(["--image", self.image])
        if self.network:
            args.extend(["--network", self.network])
        if self.security_group:
            args.extend(["--security-group", self.security_group])
        if self.server_group:
            args.extend(["--server-group", self.server_group])
        if self.server_group_policy:
            args.extend(["--server-group-policy", self.server_group_policy])

        return args


def add_instance_creation_options(parser: argparse.ArgumentParser) -> argparse.ArgumentParser:
    """Adds the common instance creation option to a parser."""
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
            "Server group to start the instance in. If it does not exist, it will create it with the given "
            "server-group-policy, will use the same as '--prefix' by default (ex. toolsbeta-test-k8s-etcd)."
        ),
    )
    parser.add_argument(
        "--server-group-policy",
        required=False,
        help=(
            "Server group policy to start the instance in. If it does not exist, it will create it with "
            "anti-affinity policy, will use the same as '--prefix' by default (ex. toolsbeta-test-k8s-etcd)."
        ),
        choices=[policy.value for policy in OpenstackServerGroupPolicy],
        default=OpenstackServerGroupPolicy.ANTI_AFFINITY.value,
    )
    return parser


def with_instance_creation_options(args: argparse.Namespace, runner: CookbookRunnerBase) -> Callable:
    """Wraps a CookbookRunnerBase to pass to it the intance creation options.

    This allows to fully encapsulate the instance creation options and avoid the need to change anything in the code
    that uses them (ex. if you add a new option to the creation options).

    Example:
    >> class MyCookbook(CookbookBase):
    >>     def agrument_parser(self) -> argparse.ArgumentParser:
    >>         my_parser = add_instance_creation_options(ArgumentParser(...))
    >>         # Add your options/arguments
    >>         my_parser.add_argument("--my-option1", default=None)
    >>         return my_parser
    >>
    >>     def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
    >>         return with_instance_creation_options(
    >>             args=args, runner=MyCookbookRunner
    >>         )(my_option1=args.my_option1, spicerack=self.spicerack)
    >>

    For a full Cookbook example, see cookbooks.wmcs.vps.create_instance_with_prefix.CreateInstanceWithPrefix.

    """
    instance_creation_opts = InstanceCreationOpts(
        prefix=args.prefix,
        flavor=args.flavor,
        image=args.image,
        network=args.network,
        security_group=args.security_group,
        server_group=args.server_group,
        server_group_policy=args.server_group_policy
    )
    return partial(runner, instance_creation_opts=instance_creation_opts)


class CreateInstanceWithPrefix(CookbookBase):
    """WMCS cookbook to create and start a new instance following a prefix."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument("--project", required=True, help="Openstack project to manage.")
        add_instance_creation_options(parser)
        parser.add_argument(
            "--ssh-retries",
            required=False,
            default=15,
            type=int,
            help=(
                "Number of time that it will try to ssh to the new instance after starting it up, it will wait for "
                "1min between tries."
            ),
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_instance_creation_options(args, CreateInstanceWithPrefixRunner,)(
            security_group=args.security_group,
            server_group=args.server_group,
            server_group_policy=args.server_group_policy,
            project=args.project,
            ssh_retries=args.ssh_retries,
            spicerack=self.spicerack,
        )


class CreateInstanceWithPrefixRunner(CookbookRunnerBase):
    """Runner for CreateInstanceWithPrefix"""

    def __init__(
        self,
        project: str,
        spicerack: Spicerack,
        instance_creation_opts: InstanceCreationOpts,
        server_group_policy: str,
        server_group: Optional[str] = None,
        security_group: Optional[str] = None,
        ssh_retries: int = 15,
    ):
        """Init"""
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn="cloudcontrol1003.wikimedia.org",
            project=project,
        )
        self.project = project
        self.prefix = instance_creation_opts.prefix
        self.flavor = instance_creation_opts.flavor
        self.network = instance_creation_opts.network
        self.image = instance_creation_opts.image
        self.server_group = server_group or self.prefix
        self.server_group_policy = server_group_policy
        self.spicerack = spicerack
        self.security_group = security_group or f"{self.project}-k8s-full-connectivity"
        self.ssh_retries = ssh_retries

    def run(self) -> CreateServerResponse:  # pylint: disable=too-many-locals
        """Main entry point"""
        self.openstack_api.security_group_ensure(
            security_group=self.security_group,
            description="This group provides full access from its members to its members.",
        )
        self.openstack_api.server_group_ensure(
            server_group=self.server_group,
            policy=OpenstackServerGroupPolicy(self.server_group_policy),
        )

        all_project_servers = self.openstack_api.server_list(print_output=False)
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
                    f"As there's no other prefix members (prefix={self.prefix}), I can't add a new member without "
                    f"explicitly specifying the missing {', '.join(missing_params)} options."
                )
                LOGGER.error(message)
                raise Exception(message)

            last_prefix_member_id = 0

        else:
            # the trimming by length of the prefix allows prefixes with trailing integers (ex. tools-sgeexec-09)
            # so 1 will be extracted as id, instead of 901 for tools-sgexec-0901
            last_prefix_member_id = max(
                int(member["Name"][len(self.prefix) :].rsplit("-", 1)[-1]) for member in other_prefix_members
            )

        new_prefix_member_name = f"{self.prefix}-{last_prefix_member_id + 1}"
        maybe_security_group = self.openstack_api.security_group_by_name(name=self.security_group, print_output=False)
        if maybe_security_group is None:
            raise Exception(
                f"Unable to find a '{self.security_group}' security group for project {self.project}, though it "
                "should have been created before if not there."
            )

        security_group_id: str = maybe_security_group["ID"]

        maybe_default_security_group = self.openstack_api.security_group_by_name(name="default", print_output=False)
        if maybe_default_security_group is None:
            raise Exception(f"Unable to find a default security group for project {self.project}")

        default_security_group_id: str = maybe_default_security_group["ID"]

        maybe_server_group = self.openstack_api.server_group_by_name(name=self.server_group, print_output=False)
        if maybe_server_group is None:
            raise Exception(
                f"Unable to find a server group with name {self.server_group} for project {self.project}, though it "
                "should have been created before if not there."
            )

        server_group_id: str = maybe_server_group["ID"]

        new_instance_id = self.openstack_api.server_create(
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
            tries=self.ssh_retries,
            delay=timedelta(minutes=1),
            backoff_mode="constant",
            exceptions=(RemoteExecutionError,),
        )
        def try_to_reach_the_new_instance():
            return run_one(node=new_prefix_node, command=["hostname"]).strip()

        result = try_to_reach_the_new_instance()

        if "mesg: ttyname failed" in result:
            # Ugly workaround for https://gerrit.wikimedia.org/r/c/operations/software/spicerack/+/730270
            run_one(
                node=new_prefix_node,
                command=["sed", "-i", "-e", "'s/mesg n || true/mesg n 2>/dev/null || true/'", "/root/.profile"],
            )

        return CreateServerResponse(server_id=new_instance_id, server_fqdn=new_instance_fqdn)
