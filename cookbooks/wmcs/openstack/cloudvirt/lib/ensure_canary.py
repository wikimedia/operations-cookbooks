r"""WMCS openstack - ensure canary VMs exists

This cookbook makes sure a canary VM exists in each of the specified cloudvirts.

Usage example: wmcs.openstack.cloudvirt.lib.ensure_canary \
    --hostname-list cloudvirt1013 cloudvirt1040
"""
import argparse
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase

from cookbooks.wmcs.libs.common import (
    CommonOpts,
    SALLogger,
    WMCSCookbookRunnerBase,
    add_common_opts,
    natural_sort_key,
    parser_type_list_hostnames,
    with_common_opts,
)
from cookbooks.wmcs.libs.inventory import get_openstack_internal_network_name
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI, OpenstackClusterName, OpenstackName, get_control_nodes

LOGGER = logging.getLogger(__name__)

FLAVOR = "g3.cores1.ram1.disk20"
IMAGE = "debian-11.0-bullseye"


class EnsureCanaryVM(CookbookBase):
    """WMCS Openstack cookbook to ensure canary VM exists."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser, project_default="cloudvirt-canary")
        parser.add_argument(
            "--deployment",
            required=False,
            choices=list(OpenstackClusterName),
            type=OpenstackClusterName,
            default=OpenstackClusterName.EQIAD1,
            help="Deployment name to operate on",
        )
        parser.add_argument(
            "--hostname-list",
            required=False,
            nargs="+",
            type=parser_type_list_hostnames,
            help="List of cloudvirt hostnames to operate on. If not present, operate on all of them",
        )
        parser.add_argument(
            "--recreate",
            required=False,
            action="store_true",
            help="If the canary VM should be forcefully recreated",
        )
        return parser

    def get_runner(self, args: argparse.Namespace) -> WMCSCookbookRunnerBase:
        """Get runner"""
        return with_common_opts(self.spicerack, args, EnsureCanaryVMRunner,)(
            hostname_list=args.hostname_list,
            deployment=args.deployment,
            recreate=args.recreate,
            spicerack=self.spicerack,
        )


@dataclass(frozen=True)
class HostChanges:
    """Class to store hypervisor changes."""

    hostname: str
    vm_prefix: str
    to_delete: List[OpenstackName]
    to_force_reboot: Optional[OpenstackName]
    needs_create: bool

    @classmethod
    def from_canary_vms(cls, hostname: str, host_canary_vms: List[Dict[str, Any]], recreate: bool) -> "HostChanges":
        """Create a HostChanges instance."""
        cloudvirt_number = hostname.split("cloudvirt")[1]
        vm_prefix = f"canary{cloudvirt_number}"

        to_delete = []
        to_force_reboot = None
        needs_create = True

        for server in host_canary_vms:
            vm_name = server["Name"]

            if recreate:
                LOGGER.debug("marking %s for deletion: recreate requested", vm_name)
                to_delete.append(vm_name)
                continue

            if not vm_name.startswith(vm_prefix):
                LOGGER.debug("marking %s for deletion: misplaced VM with wrong prefix", vm_name)
                to_delete.append(vm_name)
                continue

            if server["Flavor Name"] != FLAVOR:
                LOGGER.debug("marking %s for deletion: wrong flavor", vm_name)
                to_delete.append(vm_name)
                continue

            if server["Image Name"] != IMAGE:
                LOGGER.debug("marking %s for deletion: wrong image", vm_name)
                to_delete.append(vm_name)
                continue

            if server.get("Status", "") == "ERROR":
                # this is a common case after the hypervisor has been reimaged, the VM
                # is in error state. Try force-rebooting it (no need to create a new one)
                if to_force_reboot:
                    LOGGER.debug("marking %s for deletion: already got one in ERROR state to force-reboot", vm_name)
                    to_delete.append(vm_name)
                    continue

                if not needs_create:
                    LOGGER.debug(
                        "marking %s for deletion: we could force-reboot it but already got one ACTIVE", vm_name
                    )
                    to_delete.append(vm_name)
                    continue

                LOGGER.debug("marking %s to force-reboot", vm_name)
                to_force_reboot = vm_name
                needs_create = False
                continue

            if server.get("Status", "") != "ACTIVE":
                LOGGER.debug("marking %s for deletion: old/broken artifact", vm_name)
                to_delete.append(vm_name)
                continue

            if server.get("Status", "") == "ACTIVE":
                if needs_create:
                    LOGGER.debug("NOOP because %s seems alright", vm_name)
                    needs_create = False
                    continue

                if to_force_reboot:
                    to_delete.append(to_force_reboot)
                    to_force_reboot = None
                    LOGGER.debug(
                        "marking %s for deletion: previously to be rebooted, we later discovered an ACTIVE VM",
                        to_force_reboot,
                    )
                    continue

            to_delete.append(vm_name)
            LOGGER.debug("marking %s for deletion: couldn't find any reason to leave it alive", vm_name)

        return cls(
            hostname=hostname,
            vm_prefix=vm_prefix,
            to_delete=to_delete,
            to_force_reboot=to_force_reboot,
            needs_create=needs_create,
        )

    def has_changes(self) -> bool:
        """Returns True if there is any change represented in the class."""
        if len(self.to_delete) == 0 and not self.needs_create and not self.to_force_reboot:
            return False

        return True

    def __str__(self) -> str:
        """String representation."""
        msg = ""

        if len(self.to_delete) > 0:
            msg += "Would delete "
            msg += ",".join(self.to_delete)
            msg += " ; "

        if self.needs_create:
            msg += "Would create a new VM ; "

        if self.to_force_reboot:
            msg += f"Would force-reboot the already-present canary VM {self.to_force_reboot}"

        return msg


def calculate_changelist(
    hypervisors: List[str], existing_canary_vms: List[Dict[str, Any]], recreate: bool
) -> List[HostChanges]:
    """Helper to calculate a list of changes via HostChanges()."""
    host_to_canary_vms: Dict[str, List[Dict[str, Any]]] = {}
    for canary_vm in existing_canary_vms:
        host = canary_vm["Host"]
        host_to_canary_vms[host] = host_to_canary_vms.get(host, []) + [canary_vm]

    for hypervisor in hypervisors:
        if hypervisor not in host_to_canary_vms:
            host_to_canary_vms[hypervisor] = []

    changelist = [
        HostChanges.from_canary_vms(hostname=host, host_canary_vms=host_to_canary_vms[host], recreate=recreate)
        for host, canary_vms in host_to_canary_vms.items()
    ]
    return changelist


class EnsureCanaryVMRunner(WMCSCookbookRunnerBase):
    """Runner for EnsureCanaryVM"""

    def __init__(
        self,
        common_opts: CommonOpts,
        hostname_list: List[str],
        deployment: OpenstackClusterName,
        recreate: bool,
        spicerack: Spicerack,
    ):
        """Init"""
        self.common_opts = common_opts
        self.deployment = deployment
        self.hostname_list = hostname_list
        self.recreate = recreate
        self.control_node_fqdn = get_control_nodes(cluster_name=self.deployment)[0]
        self.disable_sal_log = False
        super().__init__(spicerack=spicerack)

        if deployment == OpenstackClusterName.CODFW1DEV:
            # the SAL we have for codfw1dev is the admin one
            sal_project = "admin"
        else:
            sal_project = common_opts.project

        self.sallogger = SALLogger(project=sal_project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg)

        self.openstack_api = OpenstackAPI(
            remote=spicerack.remote(), cluster_name=self.deployment, project=self.common_opts.project
        )

        actual_host_list = self.openstack_api.host_list(print_output=False, print_progress_bars=False)

        if not self.hostname_list:
            self.hostname_list = actual_host_list
            LOGGER.info("acting on all %d hypervisors", len(self.hostname_list))
        else:
            for hostname in self.hostname_list:
                if not hostname.startswith("cloudvirt"):
                    raise ValueError(
                        f"wrong hostname, expecting a string that starts with 'cloudvirt' but got: {hostname}"
                    )
                if hostname not in actual_host_list:
                    raise ValueError(f"wrong hostname, does not exists: {hostname}")

    def _sal_log(self, msg: str) -> None:
        """Simple wrapper function to disable the SAL logger."""
        if self.disable_sal_log:
            return

        self.sallogger.log(msg)

    def _get_new_vm_name(self, vm_prefix: str) -> str:
        """Calculate the new VM prefix."""
        other_prefix_members = list(
            sorted(
                (server for server in self.existing_canary_vms if server.get("Name", "noname").startswith(vm_prefix)),
                key=lambda server: natural_sort_key(server.get("Name", "noname-0")),
            )
        )

        if other_prefix_members:
            last_prefix_member_id = max(
                int(member["Name"][len(vm_prefix) :].rsplit("-", 1)[-1]) for member in other_prefix_members
            )
        else:
            last_prefix_member_id = 0

        return f"{vm_prefix}-{last_prefix_member_id + 1}"

    def _force_reboot(self, hostchanges: HostChanges) -> None:
        """Force reboot a given VM on a given cloudvirt."""
        if not hostchanges.to_force_reboot:
            return

        LOGGER.info("INFO: %s: force-rebooting VM %s", hostchanges.hostname, hostchanges.to_force_reboot)

        if not self.spicerack.dry_run:
            self.openstack_api.server_force_reboot(name_to_reboot=hostchanges.to_force_reboot)

        self._sal_log(f"force-rebooting VM {hostchanges.to_force_reboot} in host {hostchanges.hostname}")

    def _create(self, hostchanges: HostChanges) -> None:
        """Create canary VM on a given cloudvirt."""
        if not hostchanges.needs_create:
            return

        new_vm_name = self._get_new_vm_name(hostchanges.vm_prefix)

        LOGGER.info("INFO: %s: creating VM %s", hostchanges.hostname, new_vm_name)

        if not self.spicerack.dry_run:
            self.openstack_api.server_create(
                flavor=FLAVOR,
                image=IMAGE,
                network=get_openstack_internal_network_name(self.deployment),
                name=new_vm_name,
                properties={"description": "canary VM"},
                availability_zone=f"host:{hostchanges.hostname}",
            )

        self._sal_log(f"created VM {new_vm_name} in {hostchanges.hostname}")

    def _delete(self, hostchanges: HostChanges) -> None:
        """Delete canary VMs."""
        for vm_name in hostchanges.to_delete:
            LOGGER.info("INFO: %s: deleting VM %s", hostchanges.hostname, vm_name)

            if not self.spicerack.dry_run:
                self.openstack_api.server_delete(name_to_remove=vm_name)

            self._sal_log(f"deleted VM {vm_name} from {hostchanges.hostname}")

    def run_with_proxy(self) -> None:
        """Main entry point"""
        self.existing_canary_vms = self.openstack_api.server_list(
            long=True, print_output=False, print_progress_bars=False
        )

        changelist = calculate_changelist(self.hostname_list, self.existing_canary_vms, self.recreate)

        how_many_changes = len(changelist)
        if how_many_changes == 0:
            LOGGER.info("INFO: no changes")
            return

        if how_many_changes > 10:
            self.sallogger.log(f"performing {how_many_changes} changes to canary VMs")
            # disable further calls to the SAL
            self.disable_sal_log = True

        for hostchanges in changelist:
            if hostchanges.has_changes():
                LOGGER.info(f"INFO: {hostchanges.hostname} has changes: {hostchanges}")

            self._force_reboot(hostchanges)
            self._create(hostchanges)
            self._delete(hostchanges)
