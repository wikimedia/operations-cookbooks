"""Update Sentry PDUs 🔌 SNMP communities"""
# pylint overrides are for https://bugs.python.org/issue31844 but on 3.10 it's fixed, so adding the useless suppression
# pylint: disable=useless-suppression
from datetime import datetime
from html.parser import HTMLParser
from logging import getLogger
from secrets import token_urlsafe
from time import sleep

from requests import Session
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ensure_shell_is_durable, get_secret

from cookbooks.sre import pdus


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


class ChangeSNMP(CookbookBase):
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

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = pdus.argument_parser_base(self.__doc__)
        parser.add_argument('--force', action='store_true',
                            help='Force an update even if the current values seem correct')
        parser.add_argument('--reset-rw', action='store_true',
                            help='Reset the RW community to a random string')
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ChangeSNMPRunner(args, self.spicerack)


class ChangeSNMPRunner(CookbookRunnerBase):
    """Runner to change PDU SNMP strings."""

    def __init__(self, args, spicerack):
        """Initiate snmpe changer."""
        ensure_shell_is_durable()

        password = get_secret('Enter login password')
        self.snmp_ro = get_secret('New SNMP RO String', confirm=True)
        self.snmp_rw = token_urlsafe(16) if args.reset_rw else None

        self.force = args.force
        self.reset_rw = args.force
        self.check_default = args.check_default

        self.session = Session()
        self.session.verify = False
        self.session.auth = (args.username, password)
        self.spicerack = spicerack

        self._pdus = pdus.get_pdu_ips(spicerack.netbox(), args.query)

    def change_snmp(self, pdu: str, version: int) -> bool:
        """Change the snmp_string.

        Arguments:
            pdu (str): the pdu
            version (str): The pdu version number

        Returns:
            bool: Indicate if a string was updated
        Raises:
            SnmpResetError

        """
        version = pdus.get_version(pdu, self.session)
        parser_func = {
            3: PDUParserV3,
            4: PDUParserV4,
        }.get(version)

        if parser_func is None:
            raise SnmpResetError("Unknown Version Sentry 👎")

        parser = parser_func()
        snmp_form = 'https://{}/Forms/snmp_1'.format(pdu)

        try:
            # first fetch the form to get the current values
            logger.debug('%s: Fetch current values', pdu)
            response = pdus.get(self.session, snmp_form)
            parser.feed(response.content.decode())
        except pdus.RequestError as err:
            raise SnmpResetError from err
        form = parser.form.copy()
        # update the form paramters with new values
        if form['GetCom'] != self.snmp_ro:
            logger.info('%s: Updating SNMP RO', pdu)
            form['GetCom'] = self.snmp_ro
        if self.snmp_rw:
            logger.info('%s: Updating SNMP RW', pdu)
            form['SetCom'] = self.snmp_rw
        # post new values
        if form == parser.form:
            uptime = pdus.get_uptime(pdu, self.session)
            logger.info('%s: SNMP communities already match (version: %d, uptime: %s)',
                        pdu, version, uptime)
            if not self.force:
                return False
            logger.info('%s: Force update', pdu)
        try:
            logger.debug('Posting: %s -> %s', form, snmp_form)
            response = pdus.post(self.session, snmp_form, form)
            parser.feed(response.content.decode())
        except pdus.RequestError as err:
            raise SnmpResetError from err
        # Check the new values applied
        if parser.form['GetCom'] != self.snmp_ro:
            raise SnmpResetError('{}: failed to update snmp_ro'.format(pdu))

        logger.info('%s: SNMP RO: updated', pdu)
        if self.snmp_rw and parser.form['SetCom'] != self.snmp_rw:
            raise SnmpResetError('{}: failed to update snmp_rw'.format(pdu))
        if self.snmp_rw:
            logger.info('%s: SNMP RW: updated', pdu)
        return True

    def run(self):
        """Required by Spicerack API."""
        return_code = 0

        for pdu in self._pdus:
            version = pdus.get_version(pdu, self.session)
            try:
                if not self.spicerack.dry_run:
                    if self.change_snmp(pdu, version):
                        reboot_time = datetime.utcnow()
                        pdus.reboot(pdu, version, self.session)
                        # Reboots from experience take at least 60 seconds
                        logger.info('%s: sleep while reboot', pdu)
                        sleep(60)
                        pdus.wait_reboot_since(pdu, reboot_time, self.session)
                else:
                    logger.info('%s: Dry run, not trying.', pdu)
                if self.check_default:
                    if pdus.check_default(pdu, self.session):
                        # TODO: delete default user
                        pass
            except (pdus.VersionError, SnmpResetError, pdus.RebootError) as error:
                logger.error(error)
                return_code = 1
        return return_code
