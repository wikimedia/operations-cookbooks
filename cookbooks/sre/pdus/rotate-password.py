"""Update Sentry PDUs passwords.

- Optionally checks if the default user is still configured.
- Default user is 'root'
- If host 'all' is passed, will iterate over all PDUs
- So try --dry-run first 😉

Usage example:
    cookbook sre.hosts.rotate-pdu-password --username MrFoo 'ps1-b5-eqiad.mgmt.eqiad.wmnet'
    cookbook sre.hosts.rotate-pdu-password all
    cookbook sre.hosts.rotate-pdu-password all --check_default
"""

import logging

from argparse import ArgumentParser, RawDescriptionHelpFormatter

from requests import get, Session
from requests.exceptions import HTTPError

from spicerack.interactive import ensure_shell_is_durable, get_secret

__title__ = 'Update Sentry PDUs 🔌 passwords'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


class PasswordResetError(Exception):
    """Raised if password reset fails"""


class GetVersionError(Exception):
    """Raised if there is an issue getting PDU version"""


def argument_parser():
    """As specified by Spicerack API."""
    parser = ArgumentParser(
        description=__doc__, formatter_class=RawDescriptionHelpFormatter)
    parser.add_argument('--username', help='Username (default: %(default)s)', default='root')
    parser.add_argument('--check_default', help='Check for default user',
                        action='store_true')
    parser.add_argument('query', help='PDU FQDN or \'all\'')

    return parser


def run(args, spicerack):  # noqa: MC0001
    """Required by Spicerack API."""
    ensure_shell_is_durable()
    session = Session()
    session.verify = False
    return_code = 0
    current_password = get_secret('Current password')
    new_password = get_secret("New password", confirm=True)

    session.auth = (args.username, current_password)

    if args.query == 'all':
        netbox = spicerack.netbox()
        devices = netbox.api.dcim.devices.filter(role='pdu')
        pdus = [str(device.primary_ip).split('/')[0] for device in devices
                if device.primary_ip is not None]
    else:
        pdus = set(args.query)

    for pdu in pdus:
        try:
            if not spicerack.dry_run:
                change_password(pdu, session, new_password)
            else:
                logger.info('%s: Dry run, not trying.', pdu)
            if args.check_default:
                if check_default(pdu):
                    # TODO: delete default user
                    return_code = 1
        except GetVersionError as error:
            logger.error('%s: Failed to get PDU version: %s', pdu, str(error))
            return_code = 1
        except PasswordResetError as error:
            logger.error('%s: Failed to reset password: %s', pdu, str(error))
            return_code = 1
        except Exception as error:
            logger.error('%s: Something went wrong: %s', pdu, str(error))
    return return_code


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
        response = session.get("https://{}/chngpswd.html".format(pdu))
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


def change_password(pdu, session, new_password):
    """Change the password

    Arguments:
        pdu (str): the pdu fqdn
        session (requests.Session): A configured request session
        new_password (str): the new password to use

    Raises:
        PasswordResetError

    """
    payload = {
        3: {
            'Current_Password': session.auth[1],
            'New_Password': new_password,
            'New_Password_Verify': new_password
        },
        4: {
            'FormButton': 'Apply',
            'UPWC': session.auth[1],
            'UPW': new_password,
            'UPWV': new_password
        }
    }.get(get_version(pdu, session))

    # Then change the password
    try:
        response = session.post("https://{}/Forms/chngpswd_1".format(pdu), data=payload)
        response.raise_for_status()
    except HTTPError as err:
        raise PasswordResetError("{}: Error {} while trying to change the password: {}".format(
            pdu, response.status_code, err))

    session.auth[1] = new_password
    session.cookies.clear()
    try:
        response = session.get("https://{}/chngpswd.html".format(pdu))
        response.raise_for_status()
    except HTTPError as err:
        raise PasswordResetError(
            '{}: Error {}. New password not working, the change probably failed:\n{}'.format(
                pdu, response.status_code, err))
    logger.info('%s: Password updated successfully 😌', pdu)


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
