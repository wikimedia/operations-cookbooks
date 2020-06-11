"""PDU Operations"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from logging import getLogger
from re import match

import requests.exceptions

from requests import get
from spicerack.decorators import retry

from cookbooks import ArgparseFormatter

logger = getLogger(__name__)  # pylint: disable=invalid-name
MIN_SECRET_SIZE = 6


class VersionError(Exception):
    """Raised if there is an issue getting PDU version"""


class RebootError(Exception):
    """Exception raised if password reset fails"""


class UptimeError(Exception):
    """Exception raised if we fail to get the uptime"""


class RemoteCheckError(Exception):
    """Exception raised if we fail to get the uptime"""


def argument_parser_base():
    """As specified by Spicerack API."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('--username', help="Username to login to the PDU's", default='root')
    parser.add_argument('--check_default', help='Check for default user',
                        action='store_true')
    parser.add_argument('query', help='PDU FQDN or \'all\'')

    return parser


def check_default(pdu):
    """Checks if the default password is set on the device

    Arguments:
        pdu (str): the pdu
    Returns:
        bool: indicating if the default password is in use

    """
    response = get("https://{}/chngpswd.html".format(pdu),
                   verify=False,  # nosec
                   auth=('admn', 'admn'))
    if response.ok:
        logger.warning('%s: Default user found 😞', pdu)
        return True
    logger.info('%s: No default user 👍', pdu)
    return False


def get_version(pdu, session):
    """Check the firmware version and returns the matching Sentry version

    Arguments:
        pdu (str): the pdu fqdn
        session (requests.Session): A configured request session

    Returns:
        int: the version number

    Raises:
        VersionError

    """
    try:
        response = session.get("https://{}/".format(pdu))
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise VersionError("{}: Error {} while trying to check the version: {}".format(
            pdu, response.status_code, err))
    if 'v7' in response.headers['Server']:
        logger.debug('%s: Sentry 3 detected', pdu)
        return 3
    if 'v8' in response.headers['Server']:
        logger.debug('%s: Sentry 4 detected', pdu)
        return 4
    raise VersionError('{}: Unknown Sentry version'.format(pdu))


def reboot(pdu, version, session):
    """Reboot the PDU

    Arguments:
        pdu (str): the pdu
        version (int): the Sentry version of the PDU
        session (requests.Session): A configured request session

    """
    form = {
        3: {'Reboot_Action': 1},
        4: {'RST': '00000001', 'FormButton': 'Apply'},
    }.get(version)

    if form is None:
        raise RebootError('{}: Unknown Sentry version'.format(pdu))

    logger.info('%s: rebooting Sentry v%d PDU', pdu, version)
    try:
        response = session.get("https://{}/Forms/reboot_1".format(pdu), data=form)
        response.raise_for_status()
    except requests.exceptions.HTTPError as err:
        raise RebootError("{}: Error {} while trying to reboot : {}".format(
            pdu, response.status_code, err))


@retry(tries=25, delay=timedelta(seconds=10), backoff_mode='linear', exceptions=UptimeError)
def wait_reboot_since(pdu, since, session):
    """Poll the host until it is reachable and has an uptime lower than the provided datetime.

    Arguments:
        pdu (str): the pdu
        since (datetime.datetime): the time after which the host should have booted.
        session (requests.Session): A configured request session

    Raises:
        RemoteCheckError: if unable to connect to the host or the uptime is higher than expected.

    """
    delta = (datetime.utcnow() - since).total_seconds()
    uptime = parse_uptime(get_uptime(pdu, session))
    if uptime >= delta:
        raise UptimeError('{}: uptime is higher then threshold: {} > {}'.format(
            pdu, uptime, delta))
    logger.info('%s: found reboot since %s', pdu, since)


def get_pdu_ips(netbox):
    """Return a set of PDU IP addresses

    Arguments:
        netbox (spicerack.netbox.Netbox): A Spicerack Netbox instance

    Returns:
        set: A set of PDU IPs

    """
    devices = netbox.api.dcim.devices.filter(role='pdu')
    return set(str(device.primary_ip).split('/')[0] for device in devices
               if device.primary_ip is not None)


def get_uptime(pdu, session):
    """Return the PDU uptime

    Arguments:
        pdu (str): the pdu
        session (requests.Session): A configured request session

    Returns:
        (str): the server uptime

    """
    try:
        response = session.get("https://{}/CDU/summary.txt".format(pdu))
        response.raise_for_status()
    except requests.exceptions.ConnectionError as err:
        raise UptimeError("{}: Error while trying to check get uptime: {}".format(pdu, err))
    except requests.exceptions.HTTPError as err:
        raise UptimeError("{}: Error {} while trying to check get uptime: {}".format(
            pdu, response.status_code, err))
    # summary.text has a bunch of entries `/key(=type)?=value/` separated by pipe
    for entry in response.text.split('|'):
        tokens = entry.split('=')
        if tokens[0] == 'uptime':
            return tokens[2]
    raise UptimeError('{}: Error unable to parse uptime from summary.txt'.format(pdu))


def parse_uptime(uptime):
    """Parse the uptime to a datetime object

    Arguments:
        uptime (str): uptime as provided by the unix uptime command

    Returns:
        int: the uptime represented in seconds

    Raises:
        UptimeError: raised if unable to parse uptime

    """
    pattern = (r'(?P<days>\d+)\s+days?'
               r'\s+(?P<hours>\d+)\s+hours?'
               r'\s+(?P<minutes>\d+)\s+minutes?'
               r'\s+(?P<seconds>\d+)\s+seconds?')
    matches = match(pattern, uptime)
    if matches is None:
        raise UptimeError('unable to parse uptime: {}'.format(uptime))
    # cast values to int for timedelta
    matches = {k: int(v) for k, v in matches.groupdict().items()}
    uptime_delta = timedelta(**matches)
    return int(uptime_delta.total_seconds())
