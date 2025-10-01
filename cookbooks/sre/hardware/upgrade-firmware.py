"""Audit and if necessary update firmware on a host."""
# pylint: disable=too-many-lines
import logging
import shlex

from argparse import ArgumentTypeError
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import cache
from io import BufferedReader
from pathlib import Path
from socket import getfqdn
from subprocess import CalledProcessError, run
from tempfile import TemporaryDirectory
from typing import cast, Optional
from zipfile import ZipFile

from packaging import version

from spicerack.constants import KEYHOLDER_SOCK
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.netbox import NetboxError, NetboxServer
from spicerack.redfish import (
    ChassisResetPolicy,
    DellSCPPowerStatePolicy,
    RedfishError,
    RedfishDell,
)
from wmflib.config import load_yaml_config
from wmflib.interactive import ask_confirmation

from cookbooks.sre.hardware import (
    DellAPI,
    DellDriverType,
    DellDriverCategory,
    extract_version,
    list_picker,
)

logger = logging.getLogger(__name__)


class FirmwareUpgrade(CookbookBase):
    """Audit and possibly update firmware.

    Usage example:
        cookbook sre.hosts.firmware 'example1001*'

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--no-reboot",
            help="don't perform any reboots. Updates will not be installed unless the user preforms a manual reboot",
            action="store_true",
            default=False,
        )
        parser.add_argument(
            "--disable-cached-answers",
            help=(
                "By default this cookbook caches the answers for firmware selection.  "
                "Add this to disable the behaviour"
            ),
            action="store_true",
            default=False,
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
            "-f",
            "--force",
            help="force the upgrade even if the firmware already matches",
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
            action='append',
            choices=("bios", "idrac", "nic", "storage", "ssd"),
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
    """Firmware upgrade runner."""

    def __init__(self, args, spicerack):
        """Audit and possibly upgrade host firmware."""
        config = load_yaml_config(
            spicerack.config_dir / "cookbooks" / "sre.hardware.upgrade-firmware.yaml"
        )

        self.spicerack = spicerack
        self.firmware_store = (
            args.firmware_store
            if args.firmware_store
            else Path(config["firmware_store"])
        )
        self.no_reboot = args.no_reboot
        self.force = args.force
        self.yes = args.yes
        self.component = {"idrac", "bios"} if args.component is None else set(args.component)
        if len(self.component - {"idrac"}) > 1 and self.no_reboot:
            raise ArgumentTypeError(
                'Argument --no-reboot is only compatible when upgrading one driver at a time'
            )
        self.new = args.new
        self.cache_answers = not args.disable_cached_answers
        self._cumin_hosts = spicerack.remote().query(f'A:cumin and not P{{{getfqdn()}}}').hosts

        if self.new:
            self.hosts = [args.query]
        else:
            self.hosts = spicerack.remote().query(args.query).hosts

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
    def extract_payload(firmware: Path, payload_dir: str = "payload") -> Iterator[BufferedReader]:
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

    def _product_slug(self, netbox_host: NetboxServer) -> str:
        """Return the product slug for a specific netbox server.

        Arguments
            netbox_host: the netbox_host to lookup

        Returns:
            product_slug: a string representing the product slug e.g. poweredge-r440

        """
        # small hack to get around some slugs having the config in them e.g.
        # poweredge-r440-configc-202107
        return netbox_host.as_dict()["device_type"]["slug"].split("-config")[0]

    def _sync_firmware_store(self) -> None:
        """Sync the firmware store to all cumin hosts."""
        environment = {"SSH_AUTH_SOCK": KEYHOLDER_SOCK}
        for host in self._cumin_hosts:
            command = f"rsync --archive --rsh=ssh {self.firmware_store}/ {host}:{self.firmware_store}"
            try:
                run(shlex.split(command), check=True, env=environment)
            except CalledProcessError:
                logger.warning("unable to sync {self.firmware_store} to {host}")

    def _firmware_path(
        self, product_slug: str, driver_category: DellDriverCategory
    ) -> Path:
        """Return the folder to store files for the specific product and type.

        Arguments:
            product_slug: a string representing the product slug e.g. poweredge-r440
            driver_category: The driver category to get

        Returns:
            path: the path to store firmware files

        """
        return self.firmware_store / product_slug / driver_category.name

    def get_latest(
        self,
        product_slug: str,
        driver_type: DellDriverType,
        driver_category: DellDriverCategory,
    ) -> tuple[version.Version, Path]:
        """Download the latest idrac for the specific netbox model

        Arguments:
            product_slug: the host product slug
            driver_type: The driver type to get
            driver_category: The driver category to get

        Returns:
            A string representing the latest version and the path to the firmware

        """
        if driver_category == DellDriverCategory.SSD:
            raise NotImplementedError("SSD firmware fetch from DELL website not yet implemented")

        firmware_path = None
        product = self.dell_api.fetch(product_slug)
        drivers = product.find_driver(driver_type, driver_category)
        if not drivers:
            raise RuntimeError(f"unable to find any drivers for: {product_slug}\n"
                               "Please ensure that the slug is correct.")
        driver = list_picker(sorted(drivers))
        if len(driver.versions) > 1:
            # TODO: right now we will only have one version as I haven't worked out
            # how to get old versions
            pass
        driver_version = driver.versions.pop()
        firmware_path = (
            self._firmware_path(product_slug, driver_category)
            / driver_version.url.split("/")[-1]
        )
        if firmware_path.is_file():
            logger.info("%s: Already have: %s", product_slug, firmware_path)
        else:
            firmware_path.parent.mkdir(exist_ok=True, parents=True)
            firmware_path.parent.chmod(0o775)
            logger.info("%s: Downloading %s", product_slug, driver_version.url)
            self.dell_api.download(driver_version.url, firmware_path)
            self._sync_firmware_store()
        driver_version = driver_version.version
        return driver_version, firmware_path

    def _get_version_odata(
        self, redfish_host: RedfishDell, driver_category: DellDriverCategory, odata_id: str
    ) -> version.Version:
        """Get the current version

        Arguments:
            redfish_host: The host to act on
            driver_category: The driver category to get
            odata_id: optional odata_id if present get the version from the odata_id

        Returns:
            str: The version string matching the specific odata_id

        """
        if driver_category == DellDriverCategory.SSD:
            drive_uris = self._get_members(redfish_host, odata_id, "Drives")
            drive_versions = set()
            for drive_uri in drive_uris:
                drive = redfish_host.request("get", drive_uri).json()
                if drive["MediaType"] == "SSD":
                    drive_versions.add(version.parse("1+" + drive["Revision"][-4:]))

            return min(drive_versions)  # Return the minimum version of all the SSDs

        try:
            controller_key, version_key = {
                DellDriverCategory.NETWORK: (
                    "Controllers",
                    "FirmwarePackageVersion",
                ),
                DellDriverCategory.STORAGE: (
                    "StorageControllers",
                    "FirmwareVersion",
                ),
            }[driver_category]
        except KeyError as error:
            raise ValueError(
                f"{redfish_host.hostname}: {driver_category} not supported"
            ) from error
        data = redfish_host.request("get", odata_id).json()
        # Lets see if this is generic enough to work for more then just nics
        odata_version = data[controller_key][0][version_key]
        logger.debug(
            "%s: %s current version %s", redfish_host.hostname, odata_id, odata_version
        )
        return version.parse(odata_version)

    # TODO: consider moving to spicerack.redfish
    def get_version(
        self,
        redfish_host: RedfishDell,
        driver_category: DellDriverCategory,
        *,
        odata_id: Optional[str],
    ) -> version.Version:
        """Get the current version

        Arguments:
            redfish_host: The host to act on
            driver_category: The driver category to get
            odata_id: optional odata_id if present get the version from the odata_id

        Returns:
            str: The version string for a specific odata_id or driver_catagory

        """
        if odata_id is not None:
            return self._get_version_odata(redfish_host, driver_category, odata_id)
        try:
            return {
                DellDriverCategory.IDRAC: redfish_host.firmware_version,
                DellDriverCategory.BIOS: redfish_host.bios_version,
            }[driver_category]
        except KeyError as error:
            raise ValueError(
                f"Unsupported driver_category: {driver_category}"
            ) from error

    def upload_file(self, redfish_host: RedfishDell, file_handle: BufferedReader) -> str:
        """Upload a file to idrac via rdfish.

        Arguments:
            redfish_host: The host to act on.
            file_handle: On open file handle to the object to upload

        Returns:
            str: The location of the uploaded file on redfish

        """
        push_url = redfish_host.pushuri
        # TODO: should check if the file is already uploaded, although it doesn't error if you upload twice
        head_response = redfish_host.request("head", push_url)
        headers = {"if-match": head_response.headers["ETag"]}
        files = {"file": file_handle}
        response = cast(dict, redfish_host._upload_session.post(  # pylint: disable=protected-access
            f"https://{redfish_host.interface.ip}{push_url}",
            files=files,
            headers=headers,
        ).json())
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
    def extract_message(error: dict) -> str:
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
    def simple_update(redfish_host: RedfishDell, upload_id: str):
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
    def most_recent_member(members: list[dict], key):
        """Return the most recent member of members result from dell api.

        Members will be sorted on key and the most recent value is returned.
        The value of key is assumed to be an iso date.

        Arguments:
            members: A list of dicts returned from the dell api.
            key: The key to search on.

        Returns:
            dict: the most recent member

        """

        def sorter(element: dict) -> datetime:
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
            # Some older version of idrac reboot before returning the result
            if not with_reboot:
                raise
            print("iDrac restarting")

        retry(
            tries=120,
            delay=timedelta(seconds=20),
            backoff_mode="linear",
            exceptions=(RedfishError,),
        )(redfish_host.check_connection)()

        return retry(
            tries=60,
            delay=timedelta(seconds=30),
            backoff_mode="linear",
            exceptions=(RedfishError,),
        )(redfish_host.poll_task)(job_id)

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

    def _select_firmwarefile(
        self,
        product_slug: str,
        driver_type: DellDriverType,
        driver_category: DellDriverCategory,
        *,
        odata_id: Optional[str] = None,
    ) -> tuple[version.Version, Path]:
        """Select a list of files from ones already present on the file system

        Arguments:
            product_slug: The host product_slug.
            driver_type: The driver type to get
            driver_category: The driver category to get
            odata_id: the specific odata_id

        Returns:
            (firmware_file, version): A tuple of the selected firmware file and its version

        """
        if odata_id:
            logger.info(
                "%s: picking %s (%s) update file",
                product_slug,
                driver_category,
                odata_id.split("/")[-1],
            )
        else:
            logger.info("%s: picking %s update file", product_slug, driver_category)

        firmware_dir = self._firmware_path(product_slug, driver_category)
        if not firmware_dir.is_dir():
            return self.get_latest(product_slug, driver_type, driver_category)

        current_files = sorted(
            filter(Path.is_file, firmware_dir.iterdir()),
            key=lambda f: f.stat().st_mtime,
            reverse=True,
        )
        if not current_files:
            return self.get_latest(product_slug, driver_type, driver_category)

        selection = list_picker(current_files + ["Download new file"])

        if selection == "Download new file":
            return self.get_latest(product_slug, driver_type, driver_category)
        return extract_version(selection), cast(Path, selection)

    # create a cached version of the above function
    @cache  # pylint: disable=method-cache-max-size-none
    def _cached_select_firmwarefile(self, *args, **kargs):
        return self._select_firmwarefile(*args, **kargs)

    def _update(  # pylint: disable=too-many-arguments
        self,
        redfish_host: RedfishDell,
        netbox_host: NetboxServer,
        driver_type: DellDriverType,
        driver_category: DellDriverCategory,
        *,
        extract_payload: bool = False,
        odata_id: Optional[str] = None,
    ) -> tuple[version.Version, Optional[str]]:
        """Update the driver to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.
            driver_type: The driver type to get
            driver_category: The driver category to get
            extract_payload: if true extract the bin file from the archive
            odata_id: optional odata_id if present get the version from the odata_id

        Returns:
            (target_version, job_id): A tuple of the latest version and the update job id

        """
        logger.info("%s (%s): update", netbox_host.fqdn, driver_category.name)
        current_version = self.get_version(
            redfish_host, driver_category, odata_id=odata_id
        )
        logger.info(
            "%s (%s): current version: %s",
            netbox_host.fqdn,
            driver_category.name,
            current_version,
        )

        select_firmwarefile = '_cached_select_firmwarefile' if self.cache_answers else '_select_firmwarefile'
        product_slug = self._product_slug(netbox_host)
        target_version, firmware_file = getattr(self, select_firmwarefile)(
            product_slug, driver_type, driver_category
        )
        logger.info(
            "%s (%s): target_version: %s, current_version: %s",
            netbox_host.fqdn,
            driver_category.name,
            target_version,
            current_version,
        )
        if not self.force and target_version == current_version:
            logger.info(
                "%s (%s): Skipping already at target version %s",
                netbox_host.fqdn,
                driver_category.name,
                target_version,
            )
            return target_version, None
        if not self.force and current_version > target_version:
            logger.info(
                "%s (%s): current version %s is ahead of target version %s, use force to downgrade",
                netbox_host.fqdn,
                driver_category.name,
                current_version,
                target_version,
            )
            return target_version, None
        self._ask_confirmation(
            f"""{netbox_host.fqdn} {driver_category.name}:
            About to upload {firmware_file}, please confirm (may take up to half an hour on slow BMCs)"""
        )

        # TODO: make the following the default when everything is on 4.40+
        if redfish_host.hw_model >= 10 or redfish_host.firmware_version >= version.Version('4.40'):
            return target_version, redfish_host.upload_file(firmware_file)

        if extract_payload:
            with self.extract_payload(firmware_file) as file_handle:
                upload_id = self.upload_file(redfish_host, file_handle)
        else:
            with firmware_file.open("rb") as file_handle:
                upload_id = self.upload_file(redfish_host, file_handle)

        job_id = self.simple_update(redfish_host, upload_id)
        logger.info(
            "%s (%s): has job ID - %s", netbox_host.fqdn, driver_category.name, job_id
        )
        # TODO: do i need to do something with result?
        # print(json.dumps(result, indent=4, sort_keys=True))
        return target_version, job_id

    # After firmware has been updated there is a small period between the job finishing
    # where the endpoint returns a 503.  As such we are a bit more persistent here
    @retry(
        tries=5,
        delay=timedelta(seconds=3),
        backoff_mode="constant",
        exceptions=(RedfishError,),
    )
    def _check_version(
        self,
        redfish_host: RedfishDell,
        target_version: version.Version,
        driver_category: DellDriverCategory,
        *,
        odata_id: Optional[str] = None,
    ) -> bool:
        """Check two versions and emit appropriate logging messages"""
        current_version = self.get_version(
            redfish_host, driver_category, odata_id=odata_id
        )
        logger.info(
            "%s (%s): now at version: %s",
            redfish_host.hostname,
            driver_category.name,
            current_version,
        )
        if current_version != target_version:
            logger.error(
                "%s (%s): Something went wrong, the current version (%s) does not match the most target (%s)",
                redfish_host.hostname,
                driver_category.name,
                current_version,
                target_version,
            )
            return False
        return True

    def update_idrac(self, redfish_host: RedfishDell, netbox_host: NetboxServer) -> bool:
        """Update the idrac to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        last_reboot = redfish_host.last_reboot()
        driver_category = DellDriverCategory.IDRAC
        target_version, job_id = self._update(
            redfish_host,
            netbox_host,
            DellDriverType.FRMW,
            driver_category,
            extract_payload=True,
        )
        if redfish_host.firmware_version == target_version:
            return True

        if job_id is None:
            return False

        self.poll_id(redfish_host, job_id, True)
        # TODO: this comment needs to go elses where, or we shuld perhaps print something
        # when doing an upgrade from to 2.80+
        # for older firmware we also need to
        # racadm set idrac.webserver.HostHeaderCheck 0
        # however we have to upgrade to + 2.80 before we can set it
        # We also hit this issue when upgrading to 5.10.50.00

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
        status = self._check_version(redfish_host, target_version, driver_category)
        payload = {'Attributes': {'WebServer.1.HostHeaderCheck': 'Disabled'}}
        try:
            redfish_host.request(
                'patch',
                '/redfish/v1/Managers/iDRAC.Embedded.1/Attributes',
                json=payload,
            )
        except RedfishError as error:
            logger.error(
                '%s: Failed to update HostHeaderCheck: %s', redfish_host, error
            )
            logger.error(
                '%s: You may need to run: `racadm set idrac.webserver.HostHeaderCheck 0`',
                redfish_host,
            )
        return status

    def _reboot(self, redfish_host: RedfishDell, netbox_host: NetboxServer) -> None:
        """Reboot the host

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        self._ask_confirmation(
            f"{redfish_host.hostname}: About to reboot to apply update, please confirm"
        )
        if self.new:
            redfish_host.chassis_reset(ChassisResetPolicy.FORCE_RESTART)
        else:
            ret = self.spicerack.run_cookbook(
                "sre.hosts.reboot-single",
                [netbox_host.fqdn, "--reason", "bios upgrade"],
            )
            if ret:
                logger.error("The sre.hosts.reboot-single cookbook failed for host %s", netbox_host.fqdn)
                ask_confirmation("Are you sure you want to proceed anyway?")

    def update_bios(self, redfish_host: RedfishDell, netbox_host: NetboxServer) -> bool:
        """Update the bios to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        driver_category = DellDriverCategory.BIOS
        target_version, job_id = self._update(
            redfish_host,
            netbox_host,
            DellDriverType.BIOS,
            driver_category,
        )
        if redfish_host.bios_version == target_version:
            return True

        if job_id is None:
            logger.error('%s: no job_id for BIOS update', netbox_host.fqdn)
            return False

        if self.no_reboot:
            return True

        self._reboot(redfish_host, netbox_host)
        self.poll_id(redfish_host, job_id, True)
        return self._check_version(redfish_host, target_version, driver_category)

    def _get_members(self, redfish_host: RedfishDell, odata_id: str, key: str = "Members") -> list[str]:
        """Get a list of hw member odata.id's.

        Arguments:
            redfish_host: The redfish host to act on.
            odata_id: the odata_id to fetch members from
            key: the key to get the data from

        Returns:
            members: A list of member odata.id's

        """
        data = redfish_host.request("get", odata_id).json()
        return [member["@odata.id"] for member in data[key]]

    @staticmethod
    def _filter_storage(members: list[str]) -> Optional[str]:
        """Filter the list of storage members to a single raid controller

        Arguments:
            members: list storage controller odata uri's

        """
        # for now we just filter for raid controllers
        results = []
        for member in members:
            if member.split('/')[-1].startswith('RAID'):
                results.append(member)

        if not results:
            return None

        return list_picker(results)

    def _filter_ssds(self, redfish_host: RedfishDell, members: list[str]) -> Optional[str]:
        """Filter the list of SSDs controllers from a list of storages.

        The upgrade is applied to all disks in a controller.

        Arguments:
            redfish_host: The redfish host to act on.
            members: list storage controller odata uri's

        """
        results = []
        for member in members:
            drive_uris = self._get_members(redfish_host, member, "Drives")
            for drive_uri in drive_uris:
                drive = redfish_host.request("get", drive_uri).json()
                if drive["MediaType"] == "SSD":
                    results.append(member)
                    break  # Add the controllers that have SSD disks only once

        if not results:
            return None

        return list_picker(results)

    @staticmethod
    def _filter_network(redfish_host: RedfishDell, members: list[str]) -> Optional[str]:
        """Filter the list of network members to only the one with a link status

        Arguments:
            redfish_host: The redfish host to act on.
            members: list network adaptor odata uri's

        """
        results = {}
        for member in members:
            ports: set = set()
            try:
                member_data = redfish_host.request('get', member).json()
                for controller in member_data['Controllers']:
                    ports.update(port['@odata.id'] for port in controller['Links']['NetworkPorts'])
            except KeyError as error:
                raise RuntimeError("%s: unable to find network ports") from error
            for port in ports:
                port_data = redfish_host.request('get', port).json()
                try:
                    if port_data['LinkStatus'].lower() == 'up':
                        results[f"{member_data['Id']}: {member_data['Manufacturer']}"] = member
                except KeyError as error:
                    raise RuntimeError("%s: unable to find link status") from error

        if not results:
            return None

        selection = list_picker(list(results.keys()))
        return results[selection]

    def _get_hw_member(
        self, redfish_host: RedfishDell, driver_category: DellDriverCategory
    ) -> Optional[str]:
        """Get the member to upgrade.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.
            driver_category: The driver category to get

        Returns:
            members: A member odata.id

        """
        if driver_category == DellDriverCategory.NETWORK:
            members = self._get_members(redfish_host, "/redfish/v1/Chassis/System.Embedded.1/NetworkAdapters")
            return self._filter_network(redfish_host, members)
        if driver_category == DellDriverCategory.STORAGE:
            members = self._get_members(redfish_host, "/redfish/v1/Systems/System.Embedded.1/Storage")
            return self._filter_storage(members)
        if driver_category == DellDriverCategory.SSD:
            members = self._get_members(redfish_host, "/redfish/v1/Systems/System.Embedded.1/Storage")
            return self._filter_ssds(redfish_host, members)

        raise RuntimeError(
            f"{redfish_host.hostname}: unsupported device catagory {driver_category}"
        )

    def update_driver(  # pylint: disable=too-many-return-statements
        self,
        redfish_host: RedfishDell,
        netbox_host: NetboxServer,
        driver_category: DellDriverCategory,
    ) -> bool:
        """Update a driver to the latest version.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.
            driver_category: The driver category to get

        """
        if redfish_host.hw_model < 10 and redfish_host.firmware_version < version.Version('4'):
            logger.error('iDRAC version (%s) is too low to preform driver upgrades.  '
                         'please upgrade iDRAC first')
            return False

        member = self._get_hw_member(redfish_host, driver_category)
        if member is None:
            logger.info(
                "%s: skipping %s has no member", netbox_host.fqdn, driver_category
            )
            return True

        target_version, job_id = self._update(
            redfish_host,
            netbox_host,
            DellDriverType.FRMW,
            driver_category,
            odata_id=member,
        )
        if job_id is None:
            logger.error('%s: no job_id for member (%s)', netbox_host.fqdn, member)
            return False

        if self.no_reboot:
            logger.info('%s: skipping reboot due to no-reboot (%s)', netbox_host.fqdn, member)
            return True

        if self._get_version_odata(redfish_host, driver_category, member) == target_version:
            logger.info('%s: skipping reboot version already correct (%s)', netbox_host.fqdn, member)
            return True

        self._reboot(redfish_host, netbox_host)
        self.poll_id(redfish_host, job_id, True)
        return self._check_version(redfish_host, target_version, driver_category, odata_id=member)

    def update_ssd_driver(
        self,
        redfish_host: RedfishDell,
        netbox_host: NetboxServer,
    ) -> bool:
        """Update a driver to the latest version on all SSDs of a controller.

        Arguments:
            redfish_host: The redfish host to act on.
            netbox_host: The netbox host to act on.

        """
        if redfish_host.hw_model < 10 and redfish_host.firmware_version < version.Version('4'):
            logger.error('iDRAC version (%s) is too low to preform driver upgrades.  '
                         'please upgrade iDRAC first')
            return False

        controller = self._get_hw_member(redfish_host, DellDriverCategory.SSD)
        if controller is None:
            logger.info(
                "%s: skipping %s has no member", netbox_host.fqdn, DellDriverCategory.SSD
            )
            return True

        target_version, job_id = self._update(
            redfish_host,
            netbox_host,
            DellDriverType.FRMW,
            DellDriverCategory.SSD,
            odata_id=controller,
        )
        if job_id is None:
            logger.error('%s: no job_id for member (%s)', netbox_host.fqdn, controller)
            return False

        if self.no_reboot:
            logger.info('%s: skipping reboot due to no-reboot (%s)', netbox_host.fqdn, controller)
            return True

        drive_uris = self._get_members(redfish_host, controller, "Drives")
        to_update = False
        for drive_uri in drive_uris:
            drive = redfish_host.request("get", drive_uri).json()
            if drive["MediaType"] == "SSD":
                if not drive["Revision"].endswith(str(target_version)):
                    to_update = True

        if not to_update:
            logger.info('%s: skipping reboot version already correct for all SSDs (%s)', netbox_host.fqdn, controller)
            return True

        self._reboot(redfish_host, netbox_host)
        self.poll_id(redfish_host, job_id, True)
        drive_uris = self._get_members(redfish_host, controller, "Drives")
        failed = False
        for drive_uri in drive_uris:
            drive = redfish_host.request("get", drive_uri).json()
            if drive["MediaType"] == "SSD":
                logger.info("Drive %s has now revision %s, to be compared with target version %s (ends with)",
                            drive["Id"], drive["Revision"], target_version)
                if not drive["Revision"].endswith(str(target_version)):
                    logger.error("Drive %s was not upgraded to target version %s", drive["Id"], target_version)
                    failed = True

        return not failed

    def _redfish_host(self, hostname: str) -> Optional[RedfishDell]:
        """Fetch a redfish host from a netbox host, and make sure its compatible.

        Arguments:
            hostname: The hostname to lookup

        """
        try:
            redfish_host = self.spicerack.redfish(hostname)
        except NetboxError as error:
            logger.error("%s: Skipping: %s", hostname, error)
            return None

        if redfish_host.generation < 14:
            logger.error('%s: SKIPPING - iDRAC generation (%s) is too low to perform updates.  '
                         'please upgrade iDRAC/firmware manually',
                         redfish_host.hostname, redfish_host.generation)
            return None

        if redfish_host.hw_model < 10 and redfish_host.firmware_version < version.Version('3.30.30.30'):
            logger.error('%s: SKIPPING - iDRAC version (%s) is too low to perform updates.  '
                         'please upgrade iDRAC to version 3.30.30.30 before proceeding',
                         redfish_host.hostname, redfish_host.firmware_version)
            return None
        return redfish_host

    def run(self):
        """Required by Spicerack API."""
        lock = self.spicerack.lock()
        failures = 0
        for host in self.hosts:
            hostname = host.split(".")[0]
            with lock.acquired(f"sre.hardware.upgrade-firmware:{hostname}", concurrency=1, ttl=3600):
                failures += self._run_host(hostname)

        if failures:
            return 1

        return 0

    def _run_host(self, hostname: str) -> int:
        """Run the cookbook for a single host. Return 1 on failure, 0 on success."""
        netbox_host = self.spicerack.netbox().get_server(hostname)
        redfish_host = self._redfish_host(hostname)
        if redfish_host is None:
            return 0

        # TODO: this is a bit of a hack to populate the generation property
        # We should do this in the Redfish.__init__
        logger.info(
            "%s (Gen %d): starting", netbox_host.fqdn, redfish_host.generation
        )
        failed = False
        initial_power_state = redfish_host.get_power_state()
        # Need to power the server on for any firmware updates
        manage_power = len(self.component) > 1 or self.component != {"idrac"}

        if "idrac" in self.component:
            if self.no_reboot:
                logger.warning(
                    "%s: idrac updates will restart the idrac card regardless of the --no-reboot flags",
                    netbox_host.fqdn,
                )
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

        if "bios" in self.component:
            if not self.update_bios(redfish_host, netbox_host):
                failed = True

        if "nic" in self.component:
            if not self.update_driver(
                redfish_host, netbox_host, DellDriverCategory.NETWORK
            ):
                failed = True

        if "storage" in self.component:
            if not self.update_driver(
                redfish_host, netbox_host, DellDriverCategory.STORAGE
            ):
                failed = True

        if "ssd" in self.component:
            if not self.update_ssd_driver(redfish_host, netbox_host):
                failed = True

        if self.no_reboot:
            logging.warning(
                "%s: --no-reboot used, you must reboot the host manually",
                redfish_host.hostname,
            )

        if (
            initial_power_state == DellSCPPowerStatePolicy.OFF.value
            and manage_power
        ):
            redfish_host.chassis_reset(ChassisResetPolicy.FORCE_OFF)

        return int(failed)
