"""Hardware-specific cookbooks."""
import logging
import re

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from packaging import version
from requests import Session
from requests.exceptions import RequestException

from wmflib.interactive import ask_input

logger = logging.getLogger(__name__)
__owner_team__ = "Infrastructure Foundations"


def list_picker(options: list) -> Any:
    """Present a list of objects to the user and return the selection"""
    selection = 0
    if len(options) > 1:
        print("We have found multiple entries please pick from the list below:")
        for idx, entry in enumerate(options):
            print(f"{idx}: {entry}")
        choices = [str(i) for i in range(len(options))]
        selection = int(ask_input("Please select the entry you want", choices))
    return options[selection]


def extract_version(firmware_file: Path) -> version.Version:
    """Attempt to extract version number from firmware file"""
    if firmware_file.parent.name == "SSD":
        # SSD versions are not numeric, prefix them with 1+ to make them valid
        return version.parse("1+" + str(firmware_file).split("_")[-2])

    # The firmware file has has the driver type in the path
    try:
        pattern = {
            'IDRAC': r'(?P<version>(\d{1,2}\.){3}\d{1,3})_\w{3}$',
            'BIOS': r'(?P<version>(\d{1,2}\.){2}\d{1,2})(?:_\d+)?$',
            'NETWORK': r'(?P<version>(\d{1,2}\.){2,3}\d{1,2})(?:_\d+)?$',
        }[firmware_file.parent.name]
    except KeyError:
        raise RuntimeError(
            f'Unsupported firmware type {firmware_file.parent.name} from {firmware_file}'
        ) from None
    match = re.search(pattern, firmware_file.stem)
    if match is None:
        raise RuntimeError(f'unable to extract version from: {firmware_file}')
    return version.parse(match['version'])


# TODO: remove pylint disable once on python10
# https://bugs.python.org/issue31844
class ParseMeta(HTMLParser):  # pylint: disable=abstract-method,useless-suppression
    """simple parser to extract drivers-csrf-token meta tag"""

    def __init__(self, *args, **kwargs):
        """Init method."""
        super().__init__(*args, **kwargs)
        self.csrf_token = ''  # nosec

    def handle_starttag(self, tag, attrs):
        """Parse tags for meta object."""
        if tag != 'meta' or self.csrf_token:
            return
        in_csrf = False
        content = ''
        for name, value in attrs:
            if name == 'name' and value == 'drivers-csrf-token':
                in_csrf = True
            if name == 'content':
                content = value
        if in_csrf and content:
            self.csrf_token = content


class DellDriverType(Enum):
    """Enum to represent driver types"""

    FRMW = 1
    BIOS = 2
    DRVR = 3


class DellDriverCategory(Enum):
    """Enum to represent driver catagories"""

    IDRAC = "LC"
    BIOS = "BI"
    CPLD = "CPLD"
    CHIPSET = "CS"
    DEVICE_FIRMWARE = "FW"
    FIBER_CHANNEL = "FC"
    NETWORK = "NI"
    POWER = "PS"
    SAS_NO_RAID = "SE"
    SAS_RAID = "SF"
    SAS_DRIVE = "AS"
    SSD = "PC"
    STORAGE = "ST"
    VIDEO = "VI"


@dataclass
class DellDriverVersion:
    """Data class to hold driver versions"""

    driver_id: str
    version: version.Version
    url: str
    release: datetime

    def __hash__(self) -> int:
        """Hash function.

        Returns:
            int: a unique int representing the object

        """
        return hash(self.driver_id)


@dataclass(order=True)
class DellDriver:
    """Dataclass to hold driver objects"""

    name: str
    driver_type: DellDriverType
    category_id: str
    category_name: str
    versions: set[DellDriverVersion]

    def __str__(self) -> str:
        """Return the name for str."""
        return self.name

    @staticmethod
    def from_json(obj: dict) -> "DellDriver":
        """Create a driver from json

        Arguments:
            obj (dict): dictionary of the raw json from the dell api

        Returns:
            DellDriver: The object as a DellDriver

        """
        versions = {
            DellDriverVersion(
                driver_id=obj["DriverId"],
                version=version.parse(obj["DellVer"].split(',')[0]),
                url=obj["FileFrmtInfo"]["HttpFileLocation"],
                release=datetime.fromisoformat(obj["ReleaseDateValue"]),
            )
        }
        # TODO: list through obj['OtherVersions'] and add entrie to versions
        # need a request to the following which seems to return html
        # https://www.dell.com/support/driver/en-uk/ips/api/driverlist/getotherversion
        # so far all tests return a 415 or 404

        return DellDriver(
            name=obj["DriverName"],
            driver_type=DellDriverType[obj["Type"]],
            category_id=obj["Cat"],
            category_name=obj["CatName"],
            versions=versions,
        )

    def __hash__(self) -> int:
        """Hash function.

        Returns:
            int: a unique int representing the object

        """
        return hash((self.name, self.driver_type, self.category_id))


class DellProduct:
    """Object to hold a dell product."""

    def __init__(self, name: str) -> None:
        """The init function."""
        self.name = name
        self._drivers: set[DellDriver] = set()
        self._firmwares: set[DellDriver] = set()
        self._bioses: set[DellDriver] = set()

    def _driver_type(self, driver_type: DellDriverType) -> set[DellDriver]:
        """Return the set matching a specific driver type

        Arguments:
            driver_type (DellDriverType): The driver type you want

        Return:
            set[DellDriver]: The set opf driveres matchin driver type

        """
        return {
            DellDriverType.FRMW: self._firmwares,
            DellDriverType.BIOS: self._bioses,
            DellDriverType.DRVR: self._drivers,
        }.get(driver_type, set())

    def add_driver(self, driver: DellDriver) -> None:
        """Add a driver to the product

        Arguments:
            driver (DellDriver): The driver to add

        """
        driver_type = self._driver_type(driver.driver_type)
        if driver.name in driver_type:
            logger.warning("already have driver: %s", driver)
            return
        driver_type.add(driver)

    def find_driver(
        self, driver_type: DellDriverType, category_id: DellDriverCategory
    ) -> set[DellDriver]:
        """Find all drivers from a specific type and category.

        Arguments:
            driver_type (DellDriverType): The driver type to find.
            category_id (DellDriverCategory): The driver category to find.

        Returns
            set[DellDriver]: A set of found drivers

        """
        results = set()
        for driver in self._driver_type(driver_type):
            if category_id.value == driver.category_id:
                results.add(driver)
        return results


class DellAPIError(Exception):
    """Raise when there is a error with the dell api"""


class DellAPI:
    """Class to interface with dell json API."""

    url_base = "https://www.dell.com/support/driver/en-uk/ips/api/driverlist/fetchdriversbyproduct"

    def __init__(self, session: Session):
        """Init method.

        Arguments:
            session (Session): a session object used for preforming requests

        """
        self.session = session
        self.data = {
            "oscode": "RHEL8",
        }
        self.session.headers = {
            "X-Requested-With": "XMLHttpRequest",
        }
        # set launguage pref
        self.session.cookies["lwp"] = "c=uk&l=en&s=bsd&cs=ukbsdt1"
        self._products: dict[str, DellProduct] = {}

    def refresh_session(self):
        """Refresh the session with the lastest cookies and csrf token."""
        # populate cookie store
        self.session.get("https://www.dell.com/support/home")
        # We need to grab this page to get the csrf-token
        response = self.session.get(
            "https://www.dell.com/support/driver/es-es/ips/driverlist/product/poweredge-r430"
        )

        parser = ParseMeta()
        parser.feed(response.content.decode())
        if not parser.csrf_token:
            raise DellAPIError("Unable to find drivers-csrf-token")

        self.session.headers.update({'anti-csrf-token': parser.csrf_token})

    def get(self, product: str, force: bool = False) -> DellProduct:
        """Getter for products, includes caching.

        Arguments:
            product (str): The product short code e.g. poweredge-r430.
            force (bool): If true force fetching data from the dell api.

        Returns:
            DellProduct: The dell product matching the product string.

        """
        if product not in self._products or force:
            self._products[product] = self.fetch(product)
        return self._products[product]

    def fetch(self, product: str) -> DellProduct:
        """Fetch data from the dell api about a specific product.

        Arguments:
            product (str): The product short code e.g. poweredge-r430.

        Returns:
            DellProduct: The dell product matching the product string.

        """
        dell_product = DellProduct(product)
        three_years_ago = datetime.now() - timedelta(days=3 * 365)

        self.refresh_session()
        data = {
            "productcode": product,
            # The idrac [mostly] expects to receive an exe self extracting zip targeted for windows
            "oscode": "W12R2",
        }
        try:
            response = self.session.get(self.url_base, params=data)
            response.raise_for_status()
            if response.status_code == 204:
                raise DellAPIError("No content received from DellAPI")
            json_data = response.json()
        except RequestException as error:
            raise DellAPIError("Unable to fetch dell drivers") from error
        except ValueError as error:
            raise DellAPIError("Unable to parse json output") from error

        for driver in json_data["DriverListData"]:
            if driver["Type"] not in (d.name for d in DellDriverType):
                continue
            # ignore files older then 3 years ago
            if datetime.fromisoformat(driver["ReleaseDateValue"]) < three_years_ago:
                continue
            dell_product.add_driver(DellDriver.from_json(driver))

        return dell_product

    def download(self, url: str, save_path: Path):
        """Download url to save path using the dellapi session.

        Arguments:
            url (str): The file to download.
            save_path (Path): The location to save the file.

        """
        logger.debug("Downloading %s to %s", url, save_path)
        response = self.session.get(url)
        save_path.write_bytes(response.content)
