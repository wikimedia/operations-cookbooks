"""PDU Operations"""

from argparse import ArgumentParser
from datetime import datetime, timedelta
from logging import getLogger
from re import match

import urllib3

import requests.exceptions

from spicerack.decorators import retry

from cookbooks import ArgparseFormatter

logger = getLogger(__name__)  # pylint: disable=invalid-name
MIN_SECRET_SIZE = 6


class RequestError(Exception):
    """Raised if there is an issue making a request to the PDU"""


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
    parser.add_argument('query', help='PDU FQDN or \'all\'', nargs='+')

    return parser


def _request(method, session, url, data=None, timeout=(3, 6)):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    try:
        logger.debug('%s to: %s -> %s', method, url, data)
        response = session.request(method, url, data=data, timeout=timeout)
        response.raise_for_status()
    except (requests.exceptions.ConnectTimeout, requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError, requests.exceptions.SSLError) as err:
        raise RequestError(url) from err
    except requests.exceptions.HTTPError as err:
        raise RequestError("{} ({})".format(url, response.status_code)) from err
    return response


def get(session, url, timeout=(3, 6)):
    """Preform a get request against the pdu"""
    return _request('GET', session, url, data=None, timeout=timeout)


def post(session, url, data, timeout=(3, 6)):
    """Preform a post request against the pdu"""
    return _request('POST', session, url, data=data, timeout=timeout)


def check_default(pdu, session):
    """Checks if the default password is set on the device

    Arguments:
        pdu (str): the pdu
        session (requests.Session): A configured request session
    Returns:
        bool: indicating if the default password is in use

    """
    auth = session.auth
    session.auth = ('admn', 'admn')
    response = get(session, 'https://{}/chngpswd.html'.format(pdu))
    session.auth = auth
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
        response = get(session, "https://{}/".format(pdu))
    except RequestError as err:
        raise VersionError from err
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
        3: {'Restart_Action': 1},
        4: {'RST': '00000001', 'FormButton': 'Apply'},
    }.get(version)

    if form is None:
        raise RebootError('{}: Unknown Sentry version'.format(pdu))

    logger.info('%s: rebooting Sentry v%d PDU', pdu, version)
    try:
        url = 'https://{}/Forms/restart_1'.format(pdu)
        post(session, url, data=form)
    except requests.exceptions.HTTPError as err:
        raise RebootError from err
    try:
        # This seems to be required to do the actual reboot at least  on v3
        url = 'https://{}/restarting.html'.format(pdu)
        get(session, url, timeout=1)
    except RequestError:
        pass


@retry(tries=25, delay=timedelta(seconds=5), backoff_mode='constant', exceptions=UptimeError)
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
        raise UptimeError('{}: uptime is higher than threshold: {} > {}'.format(
            pdu, uptime, delta))
    logger.info('%s: found reboot since %s', pdu, since)


def get_pdu_ips(netbox, query):
    """Query the netbox API for pdus

    If the first element in `query` is the word 'all' return all pdus' otherwise
    filter the pdus for any devices that have a device.primary_ip or device.name
    matching one of the values in `query`

    Arguments:
        netbox (spicerack.netbox.Netbox): A Spicerack Netbox instance
        query (list): A list of pdu ipaddress/device names or the word all

    Returns:
        set: A set of PDU IPs

    """
    _query = query.copy()
    pdus = set()
    devices = netbox.api.dcim.devices.filter(role='pdu')
    if 'all' in _query[0]:
        if len(_query) > 1:
            logger.warning('`all` passed as a query argument all other values will be ignored')
        pdus = set(str(device.primary_ip).split('/')[0] for device in devices
                   if device.primary_ip is not None)
    else:
        for device in devices:
            if device.primary_ip is not None:
                primary_ip = str(device.primary_ip).split('/')[0]
                if primary_ip in _query:
                    pdus.add(primary_ip)
                    _query.remove(primary_ip)
                    continue
                if device.name in _query:
                    pdus.add(primary_ip)
                    _query.remove(device.name)
        if _query:
            logger.warning("The following PDU's from the query argument where not found: %s", ', '.join(_query))
    return pdus


def get_uptime(pdu, session):
    """Return the PDU uptime

    Arguments:
        pdu (str): the pdu
        session (requests.Session): A configured request session

    Returns:
        (str): the server uptime

    """
    try:
        response = get(session, 'https://{}/CDU/summary.txt'.format(pdu))
    except RequestError as err:
        raise UptimeError from err
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
