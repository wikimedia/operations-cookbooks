"""Decommission a host from all inventories."""
import argparse
import logging

from contextlib import contextmanager
from datetime import datetime, timedelta
from io import BufferedReader
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Dict, List, Optional, Tuple
from zipfile import ZipFile

from requests import post

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.netbox import NetboxError, NetboxServer
from spicerack.redfish import (
    ChassisResetPolicy,
    DellSCPPowerStatePolicy,
    RedfishError,
    Redfish,
)
from wmflib.config import load_yaml_config
from wmflib.interactive import ask_confirmation, ask_input

from cookbooks.sre.hardware import DellAPI, DellDriverType, DellDriverCategory

logger = logging.getLogger(__name__)


class FirmwareUpgrade(CookbookBase):
    """Audit and possibly update firmware.

    Usage example:
        cookbook sre.hosts.firmware -t T12345 'example1001*'

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--type",
        )
        parser.add_argument(
            "--yes",
            "-y",
            help="Don't prompt for confirmations",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--firmware-store",
            "-S",
            help="The location where firmware is stored",
            type=Path,
        )
        parser.add_argument(
            "--firmware-file",
            help="the firmware file to install if not provided we try to fetch the most recent",
            type=Path,
        )
        parser.add_argument(
            "-f",
            "--force",
            help="force the upgrade even if the firmware allready matches",
            action="store_true",
        )
        parser.add_argument(
            "-n",
            "--new",
            help="The server is a new server and as such not in puppetdb.",
            action="store_true",
        )
        parser.add_argument(
            "-c",
            "--component",
            help="force a specific type of upgrade: %(choices)s",
            choices=("bios", "idrac"),
        )
        parser.add_argument(
            "query",
            help="Cumin query to match the host(s) to act upon.",
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return FirmwareUpgradeRunner(args, self.spicerack)


class FirmwareUpgradeRunner(CookbookRunnerBase):
    """Decommission host runner."""

    def __init__(self, args, spicerack):
        """Decommission a host from all inventories."""
        if args.type == "all" and args.file is not None:
            raise argparse.ArgumentTypeError("--file is not valid with --type all")

        self.firmware_file = None
        if args.firmware_file:
            if not args.firmware_file.is_file:
                raise argparse.ArgumentTypeError(
                    "firmware ({args.firmware}) does not exist"
                )
            self.firmware_file = args.firmware_file.resolve()

        config = load_yaml_config(spicerack.config_dir / 'cookbooks' / 'sre.hardware.upgrade-firmware.yaml')

        self.spicerack = spicerack
        self.firmware_store = args.firmware_store if args.firmware_store else Path(config['firmware_store'])
        self.force = args.force
        self.yes = args.yes
        self.component = args.component
        self.new = args.new

        if self.new:
            self.hosts = [args.query]
        else:
            self.hosts = self.spicerack.remote().query(args.query).hosts

        for host in self.hosts:
            netbox_server = spicerack.netbox_server(host.split("."))
            manufacturer = netbox_server.as_dict()["device_type"]["manufacturer"][
                "slug"
            ]
            if netbox_server.virtual:
                raise RuntimeError(f"{host}: unable to upgrade virtual host")
            if manufacturer != "dell":
                raise RuntimeError(
                    f"{host}: unable to upgrade, unsupported manufacturer ({manufacturer})"
                )

        session = spicerack.requests_session("cookbook.sre.hardware.firmware-upgrade")
        session.proxies = spicerack.requests_proxies
        self.dell_api = DellAPI(session)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        if len(self.hosts) <= 5:
            return f"upgrade firmware for hosts {self.hosts}"
        return f"upgrade firmware for {len(self.hosts)} hosts"

    @staticmethod
    @contextmanager
    def extract_payload(firmware: Path, payload_dir: str = "payload") -> BufferedReader:
        """Context handler to provide FH to extracted firmware image

        Arguments:
            firmware (Path): Path to the firmware to upload.
            image (str): The file image to extract.

        Yields:
             BufferedReader: A file handle to the extracted file

        """
        zipfile = ZipFile(firmware)

        for name in zipfile.namelist():
            # We will see how stable this remains
            # I have seen images matching
            # firmimgFIT.d9, payload/firmimg.d7 and payload/firmimgFIT.d9
            # for bios we have
            # payload/R440-021402C.cap
            if str(Path(name).parent) == payload_dir:
                # for some reason this gives the following
                # "Unable to verify Update Package signature.",
                # extracting the file manually works further
                # assuming we have allready unziped the file
                # zipfile.open(name).read() == open(name 'rb').read()
                # with zipfile.open(image_filename ) as zip_fh:
                #    yield zip_fh
                #    break
                with TemporaryDirectory() as tmp_dir:
                    out_file = Path(zipfile.extract(name, tmp_dir))
                    zipfile.close()
                    logger.debug("extracted: %s", out_file)
                    with out_file.open("rb") as file_handle:
                        yield file_handle
                break
        else:
            raise RuntimeError(f"Unable to find firmware image in {firmware}")

    def get_latest(
        self,
        netbox_host: NetboxServer,
        driver_type: DellDriverType,
        driver_category: DellDriverCategory,
    ) -> Tuple[str, Path]:
        """Download the latest idrac for the specific netbox model

        Arguments:
            netbox_host: the netbox_host to lookup
            driver_type: The driver type to get
            driver_category: The driver category to get

        Returns:
            A string representing the latest version and the path to the firmware

        """
        firmware_path = None
        product_slug = netbox_host.as_dict()["device_type"]["slug"]
        # smal hack to get around some slugs having the config in them e.g.
        # poweredge-r440-configc-202107
        product_slug = "-".join(product_slug.split("-")[:2])
        product = self.dell_api.fetch(product_slug)
        driver = sorted(product.find_driver(driver_type, driver_category))
        selection = 0
        if len(driver) > 1:
            print("We have found multiple entries please pick from the list below:")
            for idx, entry in enumerate(driver):
                print(f"{idx}: {entry.name}")
            choices = [str(i) for i in range(len(driver))]
            selection = int(ask_input("Please select the entry you want", choices))
        driver = driver[selection]
        if len(driver.versions) > 1:
            # TODO: right now we will only have one version as I haven't worked out
            # how to get old versions
            pass
        version = driver.versions.pop()
        firmware_path = self.firmware_store / product_slug / driver_type.name / version.url.split("/")[-1]
        if firmware_path.is_file():
            logger.info("%s: Already have: %s", netbox_host.fqdn, firmware_path)
        else:
            firmware_path.parent.mkdir(exist_ok=True)
            logger.info("%s: Downloading %s", netbox_host.fqdn, version.url)
            self.dell_api.download(version.url, firmware_path)
        version = version.version.split(",")[0]
        logger.debug("%s: latest version - %s", netbox_host.fqdn, version)
        return version, firmware_path

    # TODO: consider moving to spicerack.redfish
    def get_version(
        self, redfish_host: Redfish, driver_category: DellDriverCategory
    ) -> str:
        """Get the current version

        Arguments:
            redfish_host: The host to act on
            driver_category: The driver category to get

        Returns:
            str: The idrac version string

        """
        if driver_category == DellDriverCategory.IDRAC:
            return self.get_idrac_version(redfish_host)
        if driver_category == DellDriverCategory.BIOS:
            return self.get_bios_version(redfish_host)
        raise ValueError(f"Unsupported driver_category: {driver_category}")

    # TODO: consider moving to spicerack.redfish
    @staticmethod
    def get_idrac_version(redfish_host: Redfish) -> str:
        """Get the current version

        Arguments:
            redfish_host: The host to act on

        Returns:
            str: The idrac version string

        """
        version = redfish_host.request(
            "get", "/redfish/v1/Managers/iDRAC.Embedded.1?$select=FirmwareVersion"
        ).json()["FirmwareVersion"]
        logger.debug("%s: idrac current version %s", redfish_host.fqdn, version)
        return version

    # TODO: consider moving to spicerack.redfish
    @staticmethod
    def get_bios_version(redfish_host: Redfish) -> str:
        """Get the current version

        Arguments:
            redfish_host: The host to act on

        Returns:
            str: The idrac version string

        """
        version = redfish_host.request(
            "get", "/redfish/v1/Systems/System.Embedded.1?$select=BiosVersion"
        ).json()["BiosVersion"]
        logger.debug("%s: BIOS current version %s", redfish_host.fqdn, version)
        return version

    def upload_file(self, redfish_host: Redfish, file_handle: BufferedReader) -> str:
        """Upload a file to idrac via rdfish.

        Arguments:
            redfish_host: The host to act on.
            file_handle: On open file handle to the object to upload

        Returns:
            str: The location of the uploaded file on redfish

        """
        push_url = redfish_host.pushuri
        # TODO: should check if the file is already uploaded, although it doesn't error if you upload twice
        response = redfish_host.request("head", push_url)
        headers = {"if-match": response.headers["ETag"]}
        files = {"file": file_handle}
        # BUG: timeout is not hounred by redfish.request
        # response = redfish_host.request('post', push_uri, files=files, headers=headers, timeout=(120,120))
        response = post(
            f"https://{redfish_host.fqdn}{push_url}",
            files=files,
            headers=headers,
            auth=redfish_host._http_session.auth,  # pylint: disable=protected-access
            verify=False,  # nosec
            timeout=60 * 30,  # 30 minutes
        ).json()
        if "error" in response:
            error_msg = f"{redfish_host} {self.extract_message(response['error'])}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        try:
            upload_id = response["Id"]
        except KeyError:
            raise RuntimeError(
                f"{redfish_host}: unable to find upload_id"
            ) from KeyError

        logger.debug("upload ID: %s", upload_id)
        return upload_id

    @staticmethod
    def extract_message(error: Dict) -> str:
        """Extract the error messages from the redfish response.

        Arguments:
            error: the json error object from redfish

        Returns:
            The error messages as a string

        """
        message = error["message"]
        extended_message = "\n".join(
            [
                m.get("message") or m.get("Message", "")
                for m in error.get("@Message.ExtendedInfo", [])
            ]
        )
        return f"{message}\n{extended_message}"

    @staticmethod
    def simple_update(redfish_host: Redfish, upload_id: str):
        """Upgrade firmware version to

        Arguments:
            redfish_host: The host to act on.
            upload_id: The location of the file to install

        Returns:
            str: the job_id of the update job

        """
        push_url = redfish_host.pushuri
        update_payload = {
            "ImageURI": f"{push_url}/{upload_id}",
            "@Redfish.OperationApplyTime": "OnReset",
        }
        job_id = redfish_host.submit_task(
            "/redfish/v1/UpdateService/Actions/UpdateService.SimpleUpdate",
            update_payload,
        )
        logger.debug("upload has task ID: %s", job_id)
        return job_id

    @staticmethod
    def most_recent_member(members: List[Dict], key):
        """Return the most recent member of members result from dell api.

        Members will be sorted on key and the most recent value is returned.
        The value of key is assumed to be an iso date.

        Arguments:
            members: A list of dicts returned from the dell api.
            key: The key to search on.

        Returns:
            dict: the most recent member

        """

        def sorter(element: Dict) -> datetime:
            return datetime.fromisoformat(element[key])

        return sorted(members, key=sorter)[-1]

    @staticmethod
    def poll_id(redfish_host, job_id, with_reboot=False) -> None:
        """Poll for the task ID possibly allowing for a idrac reboot.

        Arguments:
            redfish_host: The host to act on.
            job_id: the job_id of the update job
            with_reboot: if true allow the host to fail polling at least once

        """
        try:
            return redfish_host.poll_task(job_id)
        except RedfishError:
            # Some older version oif idrac reboot before returning the result
            if not with_reboot:
                raise
            print("iDrac restarting")

        retry(
            tries=120,
            delay=timedelta(seconds=20),
            backoff_mode="linear",
            exceptions=(RedfishError,),
        )(redfish_host.check_connection)()

        return redfish_host.poll_task(job_id)

    def _ask_confirmation(self, message: str) -> None:
        """Wrapper around ask confirmation

        Arguments:
            message (str): the message to ask

        """
        if not self.yes:
            ask_confirmation(message)

    def _rollback(self):
        """Preform a rollback"""
        # TODO: if evrything goes wrong perform a rollback via ipmi
        # racadm rollback iDRAC.Embedded.1-1
        # racadm rollback BIOS.Setup.1-1

    def _update(
        self,
        redfish_host: Redfish,
        netbox_host: NetboxServer,
        driver_type: DellDriverType,
        driver_category: DellDriverCategory,
        current_version: str,
        *,
        extract_payload: bool = False,
        firmware_file: Optional[Path] = None,
    ) -> Optional[str]:
        """Update the driver to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.
            driver_type: The driver type to get
            driver_category: The driver category to get
            wait_for_reboot: this is used for idrac which automatically rests idrac on install
                             most calleres are expected to preform there own reset

        Returns:
            (latest_version, current_version): a tuple of strings of the latests and current versions

        """
        latest_version = None
        if firmware_file is None:
            latest_version, firmware_file = self.get_latest(
                netbox_host, driver_type, driver_category
            )
            logger.info(
                "%s (%s): latest_version: %s, current_version: %s",
                netbox_host.fqdn,
                driver_category.name,
                latest_version,
                current_version,
            )
            if not self.force and latest_version == current_version:
                logger.info(
                    "%s (%s): Skipping already at latest version %s",
                    netbox_host.fqdn,
                    driver_category.name,
                    latest_version,
                )
                return latest_version, None
        self._ask_confirmation(
            f"{netbox_host.fqdn} {driver_category.name}: About to upload {firmware_file}, please confirm"
        )

        if extract_payload:
            with self.extract_payload(firmware_file) as file_handle:
                upload_id = self.upload_file(redfish_host, file_handle)
        else:
            with firmware_file.open("rb") as file_handle:
                upload_id = self.upload_file(redfish_host, file_handle)

        self._ask_confirmation(
            f"{netbox_host.fqdn} {driver_category.name}: About to install {upload_id}, please confirm"
        )
        job_id = self.simple_update(redfish_host, upload_id)
        logger.info(
            "%s (%s): has job ID - %s", netbox_host.fqdn, driver_category.name, job_id
        )
        # TODO: do i need to do something with result?
        # print(json.dumps(result, indent=4, sort_keys=True))
        return latest_version, job_id

    def update_idrac(self, redfish_host: Redfish, netbox_host: NetboxServer) -> None:
        """Update the idrac to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        driver_category = DellDriverCategory.IDRAC
        driver_type = DellDriverType.FRMW
        logger.info("%s (%s): update", netbox_host.fqdn, driver_category.name)
        last_reboot = redfish_host.last_reboot()
        # TODO: we should store this as some pkg_utils version parse string
        current_version = self.get_version(redfish_host, driver_category)
        latest_version, job_id = self._update(
            redfish_host,
            netbox_host,
            driver_type,
            driver_category,
            current_version,
            extract_payload=True,
            firmware_file=self.firmware_file,
        )
        if job_id is None:
            return
        self.poll_id(redfish_host, job_id, True)
        # TODO: this comment needs to go elses where, or we shuld perhaps print something
        # when doing an upgrade from to 2.80+
        # for older firmware we also need to
        # racadm set idrac.webserver.HostHeaderCheck 0
        # however we have to upgrade to + 2.80 before we can set it

        # When the host reboots its quite noisy as you also get the
        # retries from wmflib...http_session as well as wmflib...retry
        # Im sure there is a better way though
        urllib_level = logging.getLogger("urllib3").getEffectiveLevel()
        wmflib_level = logging.getLogger("wmflib").getEffectiveLevel()
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("wmflib").setLevel(logging.ERROR)
        redfish_host.wait_reboot_since(last_reboot)
        logging.getLogger("urllib3").setLevel(urllib_level)
        logging.getLogger("wmflib").setLevel(wmflib_level)
        current_version = self.get_version(redfish_host, driver_category)
        logger.info(
            "%s (%s): now at version: %s",
            netbox_host.fqdn,
            driver_category.name,
            current_version,
        )
        # If we pass a firmware file then skip the version check
        if self.firmware_file is None and current_version != latest_version:
            logger.error(
                "%s (%s): Something went wrong, the current version (%s) does not match the most recent (%s)",
                netbox_host.fqdn,
                DellDriverCategory.IDRAC.name,
                current_version,
                latest_version,
            )

    def _reboot(self, redfish_host: Redfish, netbox_host: NetboxServer) -> None:
        """Reboot the host

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        self._ask_confirmation(
            f"{redfish_host.fqdn}: About to reboot to apply update, please confirm"
        )
        if self.new:
            redfish_host.chassis_reset(ChassisResetPolicy.FORCE_RESTART)
        else:
            self.spicerack.run_cookbook(
                "sre.hosts.reboot-single",
                [netbox_host.fqdn, "--reason", "bios upgrade"],
            )

    def update_bios(self, redfish_host: Redfish, netbox_host: NetboxServer) -> None:
        """Update the bios to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        driver_category = DellDriverCategory.BIOS
        driver_type = DellDriverType.BIOS
        logger.info("%s (%s): update", netbox_host.fqdn, driver_category.name)
        # TODO: we should store this as some pkg_utils version parse string
        current_version = self.get_version(redfish_host, driver_category)
        latest_version, job_id = self._update(
            redfish_host,
            netbox_host,
            driver_type,
            driver_category,
            current_version,
            firmware_file=self.firmware_file,
        )
        if job_id is None:
            return

        self._reboot(redfish_host, netbox_host)
        self.poll_id(redfish_host, job_id, True)
        current_version = self.get_version(redfish_host, driver_category)
        logger.info(
            "%s (%s): now at version: %s",
            netbox_host.fqdn,
            driver_category.name,
            current_version,
        )
        # skip the =version check if we passed a firmware file
        if self.firmware_file is None and current_version != latest_version:
            logger.error(
                "%s (%s): Something went wrong, the current version (%s) does not match the most recent (%s)",
                netbox_host.fqdn,
                DellDriverCategory.IDRAC.name,
                current_version,
                latest_version,
            )

    def run(self):
        """Required by Spicerack API."""
        for host in self.hosts:
            hostname = host.split(".")[0]
            netbox_host = self.spicerack.netbox().get_server(hostname)
            try:
                redfish_host = self.spicerack.redfish(netbox_host.mgmt_fqdn, "root")
            except NetboxError as error:
                logger.warning("Skipping: %s", error)
                continue
            # TODO: this is a bit of a hack to populate the generation property
            # We should do this in the Redfish.__init__
            logger.info("%s (Gen %d): starting", netbox_host.fqdn, redfish_host.generation)
            initial_power_state = redfish_host.get_power_state()
            # Need to power the server on for any firmware updates
            manage_power = self.component != "idrac"
            if self.component in (None, "idrac"):
                self.update_idrac(redfish_host, netbox_host)

            if (
                initial_power_state == DellSCPPowerStatePolicy.OFF.value
                and manage_power
            ):
                logger.info("%s: host powered off, powering on", netbox_host.fqdn)
                reboot_time = datetime.now()
                redfish_host.chassis_reset(ChassisResetPolicy.ON)
                if not self.new:
                    remote = self.spicerack.remote().query(netbox_host.fqdn)
                    remote.wait_reboot_since(reboot_time, False)

            if self.component in (None, "bios"):
                self.update_bios(redfish_host, netbox_host)

            if (
                initial_power_state == DellSCPPowerStatePolicy.OFF.value
                and manage_power
            ):
                redfish_host.chassis_reset(ChassisResetPolicy.FORCE_OFF)
