"""WMCS openstack - connect to the console of a VM

Usage example: wmcs.openstack.cloudvirt.vm_console \
    --vm-name fullstack-20220613230939 \
    --project admin-monitoring

"""
import argparse
import logging
import subprocess
import sys
from typing import List

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class VMConsole(CookbookBase):
    """WMCS Openstack cookbook to connect to a VM console."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            "--project",
            required=True,
            help="Name of the project the vm is running in.",
        )
        parser.add_argument(
            "--cluster-name",
            required=False,
            choices=list(OpenstackClusterName),
            type=OpenstackClusterName,
            default=OpenstackClusterName.EQIAD1,
            help="Openstack cluster_name where the VM is hosted.",
        )
        parser.add_argument(
            "--vm-name",
            required=True,
            help="Name of the virtual machine (usually the hostname).",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return VMConsoleRunner(
            project=args.project,
            cluster_name=args.cluster_name,
            vm_name=args.vm_name,
            spicerack=self.spicerack,
        )


def _run_ssh(full_hostname: str, args: List[str]) -> int:
    cmd = ["ssh", "-t", full_hostname, *args]
    with subprocess.Popen(
        args=cmd, bufsize=0, stdin=sys.stdin, stdout=sys.stdout, stderr=sys.stderr, shell=False
    ) as proc:
        proc.wait()
        return proc.returncode


class VMConsoleRunner(CookbookRunnerBase):
    """Runner for VMConsole"""

    def __init__(
        self,
        project: str,
        cluster_name: OpenstackClusterName,
        vm_name: str,
        spicerack: Spicerack,
    ):
        """Init"""
        self.project = project
        self.vm_name = vm_name
        self.spicerack = spicerack
        self.openstack_api = OpenstackAPI(remote=spicerack.remote(), cluster_name=cluster_name, project=project)

    def run(self) -> None:
        """Main entry point"""
        vm_info = self.openstack_api.server_show(vm_name=self.vm_name)
        hypervisor_fqdn = vm_info["OS-EXT-SRV-ATTR:hypervisor_hostname"]
        libvirt_vmid = vm_info["OS-EXT-SRV-ATTR:instance_name"]
        LOGGER.info(
            "Connecting to vm %s(%s) running on %s as instance %s",
            self.vm_name,
            self.project,
            hypervisor_fqdn,
            libvirt_vmid,
        )
        sys.exit(_run_ssh(hypervisor_fqdn, args=["sudo", "-i", "virsh", "console", libvirt_vmid]))
