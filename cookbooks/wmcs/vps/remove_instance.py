"""WMCS Toolforge - Remove an instance from a project.

Usage example:
    cookbook wmcs.vps.remove_instance \
        --project toolsbeta \
        --server-name toolsbeta-k8s-test-etcd-08

"""
import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.puppet import PuppetMaster

from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, run_one_raw, with_common_opts
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class RemoveInstance(CookbookBase):
    """WMCS VPS cookbook to stop an instance."""

    title = __doc__

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
        parser.add_argument(
            "--revoke-puppet-certs",
            action="store_true",
            help="If set, the Puppet certificates of this server will be revoked on a custom Puppetmaster",
        )
        parser.add_argument(
            "--server-name",
            required=True,
            help="Name of the server to remove (without domain, ex. toolsbeta-test-k8s-etcd-9).",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, RemoveInstanceRunner,)(
            name_to_remove=args.server_name,
            revoke_puppet_certs=args.revoke_puppet_certs,
            spicerack=self.spicerack,
        )


class RemoveInstanceRunner(CookbookRunnerBase):
    """Runner for RemoveInstance."""

    def __init__(
        self,
        common_opts: CommonOpts,
        name_to_remove: str,
        revoke_puppet_certs: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(),
            control_node_fqdn="cloudcontrol1003.wikimedia.org",
            project=self.common_opts.project,
        )

        self.name_to_remove = name_to_remove
        self.revoke_puppet_certs = revoke_puppet_certs
        self.spicerack = spicerack
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )

    def run(self) -> None:
        """Main entry point"""
        if not self.openstack_api.server_exists(self.name_to_remove, print_output=False):
            LOGGER.warning(
                "Unable to find server %s in project %s. Please review the project and server name.",
                self.name_to_remove,
                self.common_opts.project,
            )
            return

        if self.revoke_puppet_certs:
            node_fqdn = f"{self.name_to_remove}.{self.common_opts.project}.eqiad1.wikimedia.cloud"
            remote = self.spicerack.remote().query(f"D{{{node_fqdn}}}", use_sudo=True)

            puppet = self.spicerack.puppet(remote)
            puppet.disable(self.spicerack.admin_reason("host is being removed"))

            try:
                # for legacy VMs in .eqiad.wmflabs
                result = run_one_raw(
                    command=["hostname", "-f"], node=remote, print_output=False, print_progress_bars=False
                )

                # idk why this is needed but it filters out 'mesg: ttyname failed: Inappropriate ioctl for device'
                hostname = [
                    line
                    for line in result.splitlines()
                    if line.endswith(".wikimedia.cloud") or line.endswith(".wmflabs")
                ][0]
            except StopIteration:
                LOGGER.warning("Failed to query the hostname, falling back to the generated one")
                hostname = node_fqdn

            puppet_master_hostname = puppet.get_ca_servers()[node_fqdn]

            # if it's the central puppetmaster, this will be handled by wmf_sink
            if puppet_master_hostname not in ("puppet", "puppetmaster.cloudinfra.wmflabs.org"):
                puppet_master = PuppetMaster(
                    self.spicerack.remote().query(f"D{{{puppet_master_hostname}}}", use_sudo=True)
                )
                puppet_master.delete(hostname)

        self.sallogger.log(message=f"removing instance {self.name_to_remove}")
        self.openstack_api.server_delete(name_to_remove=self.name_to_remove)
