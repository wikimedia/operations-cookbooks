"""Update Sentry PDUs passwords.

- Optionally checks if the default user is still configured.
- Default user is 'root'
- If host 'all' is passed, will iterate over all PDUs
- So try --dry-run first 😉

Usage example:
    cookbook sre.hosts.rotate-pdu-password --username MrFoo --check_default 'ps1-b5-eqiad.mgmt.eqiad.wmnet'
    cookbook sre.hosts.rotate-pdu-password all
"""

import argparse
import getpass
import logging
import requests
import urllib3

__title__ = 'Update Sentry PDUs 🔌 passwords'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name
urllib3.disable_warnings()  # Mute insecure TLS warning (as PDUs certs are self signed)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--username', help='Username (default: %(default)s)', default='root')
    parser.add_argument('--check_default', help='Check for default user',
                        action='store_true')
    parser.add_argument('query', help='PDU FQDN or \'all\'')

    return parser


def run(args, spicerack):  # noqa: MC0001
    """Required by Spicerack API."""
    current_password = getpass.getpass("Current password:")
    if not current_password:
        logger.error('Can\'t be empty 👎')
        return 1
    new_password = getpass.getpass("New password:")
    new_password_bis = getpass.getpass("Again, just to be sure:")
    if new_password != new_password_bis:
        logger.error('They don\'t match 👎')
        return 1

    pdus = []
    return_code = 0
    if args.query == 'all':
        logger.info('Query PDUs list from PuppetDB')
        puppet_db_fqdn = spicerack.remote().query('A:puppetdb').hosts[0]
        three_phases_url = 'https://{puppet_db_fqdn}/pdb/query/v4/resources/Facilities::Monitor_pdu_3phase'.format(
                            puppet_db_fqdn=puppet_db_fqdn)
        monophase_url = 'https://{puppet_db_fqdn}/pdb/query/v4/resources/Facilities::Monitor_pdu_1phase'.format(
                         puppet_db_fqdn=puppet_db_fqdn)
        three_phases = requests.get(three_phases_url).json()
        monophase = requests.get(monophase_url).json()
        for pdu in three_phases + monophase:
            pdu_fqdn = '{hostname}.mgmt.{site}.wmnet'.format(hostname=pdu['title'], site=pdu['parameters']['site'])
            if pdu_fqdn not in pdus:
                pdus.append(pdu_fqdn)
    else:
        pdus.append(args.query)

    for pdu_fqdn in pdus:
        try:
            if not spicerack.dry_run:
                if change_password(pdu_fqdn, args.username, current_password, new_password) == 1:
                    return_code = 1
            else:
                logger.info('{pdu_fqdn}: Dry run, not trying.'.format(pdu_fqdn=pdu_fqdn))
            if args.check_default:
                if check_default(pdu_fqdn) == 1:
                    return_code = 1
        except Exception as e:
            logger.error('{pdu_fqdn}: Something went wrong: {error}'.format(pdu_fqdn=pdu_fqdn, error=str(e)))

    return return_code


def get_version(pdu_fqdn, username, password):
    """Check the firmware version and returns the matching Sentry version

    Returns:
        int: Sentry version
        or None

    """
    response_version_check = requests.get("https://{fqdn}/chngpswd.html".format(fqdn=pdu_fqdn),
                                          verify=False,  # nosec
                                          auth=(username, password))
    if response_version_check.status_code == 200:
        if 'v7' in response_version_check.headers['Server']:
            return 3
            logger.debug('{pdu_fqdn}: Sentry 3 detected'.format(pdu_fqdn=pdu_fqdn))
        elif 'v8' in response_version_check.headers['Server']:
            logger.debug('{pdu_fqdn}: Sentry 4 detected'.format(pdu_fqdn=pdu_fqdn))
            return 4
        else:
            logger.error('{pdu_fqdn}: Unknown Sentry version'.format(pdu_fqdn=pdu_fqdn))
            return 0
    else:
        logger.error("{pdu_fqdn}: Error {status_code} while trying to check the version.".format(
            status_code=response_version_check.status_code, pdu_fqdn=pdu_fqdn))


def change_password(pdu_fqdn, username, current_password, new_password):
    """Change the password

    Returns:
        1 if any issue.
        0 if all good.

    """
    version = get_version(pdu_fqdn, username, current_password)
    if version == 3:
        payload = {'Current_Password': current_password,
                   'New_Password': new_password,
                   'New_Password_Verify': new_password}
    elif version == 4:
        payload = {'FormButton': 'Apply',
                   'UPWC': current_password,
                   'UPW': new_password,
                   'UPWV': new_password}
    else:
        return 1

    with requests.Session() as session:
        session.auth = (username, current_password)

        # Initialize the session with any page to get the cookie settings
        session.get("https://{fqdn}/chngpswd.html".format(fqdn=pdu_fqdn), verify=False)  # nosec

        # Then change the password
        response_change_pw = session.post("https://{fqdn}/Forms/chngpswd_1".format(fqdn=pdu_fqdn),
                                          data=payload,
                                          verify=False)  # nosec

    if response_change_pw.status_code == 200:
        response_check_success = requests.get("https://{fqdn}/chngpswd.html".format(
                                    fqdn=pdu_fqdn),
                                              verify=False,  # nosec
                                              auth=(username, new_password))
        if response_check_success.status_code == 200:
            logger.info('{pdu_fqdn}: Password updated successfully 😌'.format(pdu_fqdn=pdu_fqdn))
            return 0
        else:
            logger.error('{pdu_fqdn}: Error {status_code}. New password not working, \
the change probably failed'.format(status_code=response_check_success.status_code, pdu_fqdn=pdu_fqdn))
            return 1
    else:
        logger.error("{pdu_fqdn}: Error {status_code} while trying to change the password.".format(
            status_code=response_change_pw.status_code, pdu_fqdn=pdu_fqdn))
        return 1


def check_default(pdu_fqdn):
    """Checks if the default password is set on the device

    Returns:
        1 if default user found.
        0 if all good.

    """
    response = requests.get("https://{fqdn}/chngpswd.html".format(fqdn=pdu_fqdn),
                            verify=False,  # nosec
                            auth=('admn', 'admn'))
    if response.status_code == 200:
        logger.warning('{pdu_fqdn}: Default user found 😞'.format(pdu_fqdn=pdu_fqdn))
        return 1
    else:
        logger.info('{pdu_fqdn}: No default user 👍'.format(pdu_fqdn=pdu_fqdn))
        return 0
