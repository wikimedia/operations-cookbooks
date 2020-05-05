"""PDU Operations"""
from argparse import ArgumentParser
from logging import getLogger

from requests import get
from requests.exceptions import HTTPError

from cookbooks import ArgparseFormatter

logger = getLogger(__name__)  # pylint: disable=invalid-name
MIN_SECRET_SIZE = 6


class GetVersionError(Exception):
    """Raised if there is an issue getting PDU version"""


class PdusRestartException(Exception):
    """Exception raised if password reset fails"""


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
        GetVersionError

    """
    try:
        response = session.get("https://{}/".format(pdu))
        response.raise_for_status()
    except HTTPError as err:
        raise GetVersionError("{}: Error {} while trying to check the version: {}".format(
            pdu, response.status_code, err))
    if 'v7' in response.headers['Server']:
        logger.debug('%s: Sentry 3 detected', pdu)
        return 3
    if 'v8' in response.headers['Server']:
        logger.debug('%s: Sentry 4 detected', pdu)
        return 4
    raise GetVersionError('{}: Unknown Sentry version'.format(pdu))


def restart(pdu, version, session):
    """Restart the PDU

    Arguments:
        pdu (str): the pdu
        version (int): the Sentry version of the PDU
        session (requests.Session): A configured request session

    """
    form = {
        3: {'Restart_Action': 1},
        4: {'RST': '00000001', 'FormButton': 'Apply'},
    }.get(version)

    if form is None:
        raise PdusRestartException('{}: Unknown Sentry version'.format(pdu))

    logger.info('%s: restarting Sentry v%d PDU', pdu, version)
    try:
        response = session.get("https://{}/Forms/restart_1".format(pdu), data=form)
        response.raise_for_status()
    except HTTPError as err:
        raise PdusRestartException("{}: Error {} while trying to check the version: {}".format(
            pdu, response.status_code, err))


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
