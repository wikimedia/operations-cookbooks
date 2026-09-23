import json
import logging
from argparse import ArgumentParser, Namespace
from datetime import timedelta
from spicerack import Spicerack
from spicerack.cookbook import (
    CookbookBase,
    CookbookRunnerBase,
)
from spicerack.remote import RemoteExecutionError, RemoteHosts
from spicerack.decorators import retry
from wmflib.interactive import (
    ask_confirmation,
    ensure_shell_is_durable,
)
from cookbooks.sre.ceph import CLUSTER_ADMIN_HOST, CLUSTER_CHOICES

logger = logging.getLogger(__name__)


class RemoveCephOsd(CookbookBase):
    """Remove an osd from a ceph host.

    This cookbook should be used when the underlying disk backing an OSD is failing / has failed,
    in order to relocate the assigned placement groups to other OSDs within the cluster, as well
    as remove the failed OSD.


    Usage example:
        cookbook sre.ceph.remove-sd \
            --cluster cephosd-eqiad \
            --osd 24 \
            --task-id T12345 \
            --reason 'disk failed'

    """

    def get_runner(self, args: Namespace) -> "RemoveCephOsdRunner":
        """As specified by Spicerack API."""
        return RemoveCephOsdRunner(args, self.spicerack)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser: ArgumentParser = super().argument_parser()
        parser.add_argument("--cluster", choices=CLUSTER_CHOICES, help="The Ceph cluster to which the host belongs")
        parser.add_argument("--osd", type=int, help="The OSD numerical id")
        return parser


class RemoveCephOsdRunner(CookbookRunnerBase):
    """Evacuate and remove an OSD from a Ceph cluster"""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Evacuate and remove an OSD from a Ceph cluster"""
        self.spicerack = spicerack
        self.cluster = args.cluster
        self.ceph_admin_host = CLUSTER_ADMIN_HOST[args.cluster]
        self.remote_ceph_admin_host = self.spicerack.remote().query(self.ceph_admin_host)
        self.osd = args.osd
        self.osd_systemd_service = f"ceph-osd@{self.osd}.service"

    def capture_output(self, cmd: str, host: RemoteHosts) -> str:
        for _, output in host.run_sync(cmd, is_safe=True, print_output=False, print_progress_bars=False):
            return output.message().decode().strip()
        return ""

    @staticmethod
    def fail(msg: str):
        raise RuntimeError(f"{msg}. Aborting.")

    def get_osd_host(self) -> bool:
        osd_metadata_str = self.capture_output(f"ceph osd metadata {self.osd}", host=self.remote_ceph_admin_host)
        try:
            osd_metadata = json.loads(osd_metadata_str)
        except json.JSONDecodeError:
            raise RuntimeError(f"No Ceph metadata found for OSD {self.osd}")
        return osd_metadata["hostname"]

    def osd_in_cluster(self) -> bool:
        cluster_osds = self.capture_output("ceph osd ls", host=self.remote_ceph_admin_host)
        return str(self.osd) in cluster_osds.splitlines()

    def ensure_osd_volume_exists(self, device: str):
        return self.remote_ceph_host.run_sync(f"ceph-volume lvm list {device}")

    def osd_currently_running(self) -> bool:
        return (
            self.capture_output(f"systemctl is-active {self.osd_systemd_service}", host=self.remote_ceph_host)
            == "active"
        )

    def osd_ok_to_stop(self) -> bool:
        ok_to_stop_output = self.capture_output(f"ceph osd ok-to-stop osd.{self.osd}", host=self.remote_ceph_admin_host)
        ok_to_stop = json.loads(ok_to_stop_output)
        return ok_to_stop["ok_to_stop"]

    def prechecks(self):
        if not self.spicerack.dry_run:
            ensure_shell_is_durable()

        # Ensure the OSD exists and isn't already deleted
        if not self.osd_in_cluster():
            self.fail(f"OSD {self.osd} is not in the ceph cluster")
        if not self.osd_currently_running():
            self.fail(f"OSD {self.osd} is not currently running")
        if not self.osd_ok_to_stop():
            self.fail(f"OSD {self.osd} is not considered ok-to-stop in ceph")

        ask_confirmation(f"Ready to remove osd {self.osd} from {self.osd_host_fqdn}. Proceed?")

    @retry(
        tries=288,  # 288 * 5 minutes = 1 day
        delay=timedelta(seconds=60 * 5),
        backoff_mode="constant",
        exceptions=(RemoteExecutionError,),
    )
    def wait_until_all_pgs_are_remapped(self):
        return self.remote_ceph_admin_host.run_sync(f"ceph osd safe-to-destroy osd.{self.osd}")

    def get_osd_location_data(self) -> str:
        logger.info(f"Getting osd.{self.osd} location data")
        location_data = json.loads(self.capture_output("ceph-osd-locations --json", host=self.remote_ceph_host))
        for device_location_data in location_data:
            if device_location_data["osd"] == f"osd.{self.osd}":
                wwn = device_location_data["id_wwn"].lower()
                if not wwn.startswith("wwn-0x"):
                    wwn = f"wwn-0x{wwn}"
                disk = f"/dev/disk/by-id/{wwn}"
                self.remote_ceph_host.run_sync(
                    f"ls {disk}", is_safe=True, print_output=False, print_progress_bars=False
                )
                logger.info(
                    f"osd.{self.osd} is associated with device {device_location_data['device']} "
                    f"which maps to the {device_location_data['perccli']['medium']} "
                    f"located in slot {device_location_data['perccli']['location']}"
                )
                return disk
        raise RuntimeError(f"osd.{self.osd} location not found in the ceph-osd-locations output")

    def run(self):
        try:
            osd_host = self.get_osd_host()
        except RemoteExecutionError:
            self.fail(f"Coudn't map OSD {self.osd} to a hostname")

        _, site = self.ceph_admin_host.split(".", 1)
        self.osd_host_fqdn = f"{osd_host}.{site}"
        logger.info(f"OSD {self.osd} assigned to {self.osd_host_fqdn}")

        self.remote_ceph_host: RemoteHosts = self.spicerack.remote().query(self.osd_host_fqdn)
        if len(self.remote_ceph_host) == 0:
            raise RuntimeError("Specified server not found, bailing out")

        self.prechecks()

        logger.info(f"Mapping osd.{self.osd} to its physical device location")
        osd_device = self.get_osd_location_data()
        self.ensure_osd_volume_exists(device=osd_device)

        logger.info(f"Marking osd {self.osd} as out")
        self.remote_ceph_admin_host.run_sync(f"ceph osd out {self.osd}")

        logger.info(f"Checking if OSD {self.osd} is safe to destroy")
        if not self.spicerack.dry_run:
            self.wait_until_all_pgs_are_remapped()

        logger.info(f"Stopping {self.osd_systemd_service} service")
        self.remote_ceph_host.run_sync(f"systemctl stop {self.osd_systemd_service}")

        logger.info(f"Removing osd.{self.osd} from the CRUSH rules")
        self.remote_ceph_admin_host.run_sync(f"ceph osd crush remove osd.{self.osd}")

        logger.info(f"Removing osd.{self.osd} from the auth database")
        self.remote_ceph_admin_host.run_sync(f"ceph auth del osd.{self.osd}")

        logger.info(f"Purging osd.{self.osd}")
        self.remote_ceph_admin_host.run_sync(f"ceph osd purge {self.osd} --yes-i-really-mean-it")

        logger.info(f"Unmounting /var/lib/ceph/osd/ceph-{self.osd}")
        self.remote_ceph_host.run_sync(f"umount /var/lib/ceph/osd/ceph-{self.osd}")

        logger.info(f"Removing /var/lib/ceph/osd/ceph-{self.osd}")
        self.remote_ceph_host.run_sync(f"rm -fr /var/lib/ceph/osd/ceph-{self.osd}")

        logger.info(f"Zapping OSD device {osd_device}")
        self.remote_ceph_host.run_sync(f"ceph-volume lvm zap {osd_device} --destroy")
