"""Update Sentry PDUs SNMP communities.

- Optionally checks if the default user is still configured.
- Default user is 'root'
- If host 'all' is passed, will iterate over all PDUs
- So try --dry-run first 😉

Usage example:
    cookbook sre.pdus.rotate-snmp --username MrFoo 'ps1-b5-eqiad.mgmt.eqiad.wmnet'
    cookbook sre.pdus.rotate-snmp --no-ro all
    cookbook sre.pdus.rotate-snmp --no-rw all --check_default
"""
# pylint overrides are for https://bugs.python.org/issue31844 but on 3.10 it's fixed, so adding the useless suppression
# pylint: disable=useless-suppression
from base64 import b64encode
from datetime import datetime
from html.parser import HTMLParser
from logging import getLogger
from os import urandom
from time import sleep

from requests import Session
from wmflib.interactive import ensure_shell_is_durable, get_secret

from cookbooks.sre import pdus

__title__ = 'Update Sentry PDUs 🔌 SNMP communities'
logger = getLogger(__name__)


class SnmpResetError(Exception):
    """Raised if SNMP reset fails"""


class PDUParser(HTMLParser):  # pylint: disable=abstract-method
    """Base class for parsing PDU pages"""

    form_params = ['GetCom', 'SetCom', 'SysName', 'SysLoc', 'SysContact']
    _form = {
        'TrapCom': 'trap',
        'TrapUser': '',
        'Trap1': '',
        'Trap2': '',
        'TrapTime': 60,
    }

    def handle_starttag(self, tag, attrs):
        """Parse html start tags"""
        for key in self.form_params:
            if ('name', key) in attrs:
                for name, value in attrs:
                    if name == 'value':
                        setattr(self, key, value)

    @property
    def form(self):
        """Return a form dictionary"""
        for key in self.form_params:
            self._form[key] = vars(self)[key]
        return self._form


class PDUParserV3(PDUParser):  # pylint: disable=abstract-method
    """Class for parsing Sentry v3 pages"""

    def __init__(self):
        """Initialise object"""
        super().__init__()
        self._form.update({
            'SNMPv2': 0,
            'SNMPv3': 1,
            'RWUser': '',
            'RWAType': 0,
            'RWAChk': 'on',
            'RWAPass': '',
            'RWPType': 0,
            'RWPChk': 'on',
            'RWPPass': '',
            'ROUser': '',
            'ROAType': 0,
            'ROAChk': 'on',
            'ROAPass': '',
            'ROPType': 0,
            'ROPChk': 'on',
            'ROPPass': '',
            'IP_Restrict': 0,
        })


class PDUParserV4(PDUParser):  # pylint: disable=abstract-method
    """Class for parsing Sentry v4 pages"""

    def __init__(self):
        """Initialise object"""
        super().__init__()
        self._form.update({
            'FormButton': 'Apply',
            'SNMPv2': 'on',
            'TFM': '00000000',
            'IPR': '00000000',
        })


def random_string(string_length=16):
    """Return a random string of a specific length

    Arguments:
        string_length (int): The length of the random string to generate

    Returns:
        str: a random string

    """
    # gut feeling that _- will be safer then += in theory either is fine
    altchars = b'_-'
    # TODO: use secrets once we no longer need to support 3.5
    return b64encode(urandom(string_length), altchars)[:string_length]


def change_snmp(pdu, version, session, snmp_ro, snmp_rw=None, force=False):
    """Change the snmp_string

    Arguments:
        pdu (str): the pdu
        version (int): the sentry version number
        session (requests.Session): A configured request session
        snmp_ro (str): The new SNMP RO string
        snmp_rw (str): The new SNMP RW string

    Returns:
        bool: Indicate if a string was updated
    Raises:
        SnmpResetError

    """
    parser = {
        3: PDUParserV3,
        4: PDUParserV4,
    }.get(version)()

    if parser is None:
        raise SnmpResetError("Unknown Version Sentry 👎")
    if not snmp_ro and not snmp_rw:
        raise SnmpResetError("you must provide one of snmp_ro or snmp_rw")

    snmp_form = 'https://{}/Forms/snmp_1'.format(pdu)

    try:
        # first fetch the form to get the current values
        logger.debug('%s: Fetch current values', pdu)
        response = pdus.get(session, snmp_form)
        parser.feed(response.content.decode())
    except pdus.RequestError as err:
        raise SnmpResetError from err
    form = parser.form.copy()
    # update the form paramters with new values
    if form['GetCom'] != snmp_ro:
        logger.info('%s: Updating SNMP RO', pdu)
        form['GetCom'] = snmp_ro
    if snmp_rw:
        logger.info('%s: Updating SNMP RW', pdu)
        form['SetCom'] = snmp_rw
    # post new values
    if form == parser.form:
        uptime = pdus.get_uptime(pdu, session)
        logger.info('%s: SNMP communities already match (version: %d, uptime: %s)',
                    pdu, version, uptime)
        if not force:
            return False
        logger.info('%s: Force update', pdu)
    try:
        logger.debug('Posting: %s -> %s', form, snmp_form)
        response = pdus.post(session, snmp_form, form)
        parser.feed(response.content.decode())
    except pdus.RequestError as err:
        raise SnmpResetError from err
    # Check the new values applied
    if parser.form['GetCom'] != snmp_ro:
        raise SnmpResetError('{}: failed to update snmp_ro'.format(pdu))

    logger.info('%s: SNMP RO: updated', pdu)
    if snmp_rw and parser.form['SetCom'] != snmp_rw:
        raise SnmpResetError('{}: failed to update snmp_rw'.format(pdu))
    if snmp_rw:
        logger.info('%s: SNMP RW: updated', pdu)
    return True


def argument_parser():
    """As specified by Spicerack API."""
    parser = pdus.argument_parser_base()
    parser.add_argument('--force', action='store_true',
                        help='Reset the RW community to a random string')
    parser.add_argument('--reset-rw', action='store_true',
                        help='Reset the RW community to a random string')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    ensure_shell_is_durable()
    return_code = 0
    session = Session()
    session.verify = False
    password = get_secret('Enter login password')
    snmp_ro = get_secret('New SNMP RO String', confirm=True)

    session.auth = (args.username, password)

    _pdus = pdus.get_pdu_ips(spicerack.netbox(), args.query)

    for pdu in _pdus:
        snmp_rw = random_string() if args.reset_rw else None
        try:
            if not spicerack.dry_run:
                version = pdus.get_version(pdu, session)
                if change_snmp(pdu, version, session, snmp_ro, snmp_rw, args.force):
                    reboot_time = datetime.utcnow()
                    pdus.reboot(pdu, version, session)
                    # Reboots from experience take at least 60 seconds
                    logger.info('%s: sleep while reboot', pdu)
                    sleep(60)
                    pdus.wait_reboot_since(pdu, reboot_time, session)
            else:
                logger.info('%s: Dry run, not trying.', pdu)
            if args.check_default:
                if pdus.check_default(pdu, session):
                    # TODO: delete default user
                    pass
        except (pdus.VersionError, SnmpResetError, pdus.RebootError) as error:
            logger.error(error)
            return_code = 1
    return return_code
