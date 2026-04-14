"""Check the redundancy distribution of VMs in the PoPs."""
import re
from collections import defaultdict

from spicerack.cookbook import CookbookBase, CookbookInitSuccess, CookbookRunnerBase
from spicerack.remote import RemoteError
from wmflib.constants import ALL_DATACENTERS, CORE_DATACENTERS


class PopVmRedundancy(CookbookBase):
    """Check the redundancy distribution of VMs in the PoPs.

    Usage:

        cookbook sre.ganeti.pop-vm-redundancy

    """

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return PopVmRedundancyRunner(args, self.spicerack)


class PopVmRedundancyRunner(CookbookRunnerBase):
    """Check the redundancy distribution of VMs in the PoPs."""

    def __init__(self, args, spicerack):
        """Perform the check."""
        self.netbox = spicerack.netbox()
        self.remote = spicerack.remote()
        sites = set(ALL_DATACENTERS) - set(CORE_DATACENTERS)
        for site in sites:
            self.check_site(site)
        raise CookbookInitSuccess()

    def check_site(self, site: str):
        """Check a single site."""
        racks = self.netbox.api.dcim.racks.filter(site=site)
        print(f"# Checking site: {site}")
        for rack in racks:
            grouped_by_rack = defaultdict(list)
            devices = self.netbox.api.dcim.devices.filter(rack_id=rack.id, name__isw="ganeti")
            for device in devices:
                host = device.name
                grouped_by_device = defaultdict(list)
                try:
                    vms = self.remote.query(rf"F:lldp.parent ~ '{host}\..*'").hosts
                except RemoteError:
                    print(f"  🟡 Found NO VMs on host '{host}' in rack '{rack}'")
                    continue

                for vm in vms:
                    group, name, _ = re.split(r"(\d.*)", vm.split(".", 1)[0])
                    grouped_by_device[group].append(f"{group}{name}")

                for group, vms in grouped_by_device.items():
                    if len(vms) > 1:
                        print(f"  💥 Found multiple VMs of the same group '{group}' in the same host "
                              f"'{host}' in rack '{rack}': {vms}")
                    else:
                        grouped_by_rack[group].append(vms[0])

            for group, vms in grouped_by_rack.items():
                if len(vms) > 1:
                    print(f"  ❗ Found multiple VMs of the same group '{group}' in the same rack '{rack}': {vms}")

    def run(self):
        """Required by Spicerack APIs."""
