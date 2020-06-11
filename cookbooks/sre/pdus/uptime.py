"""List the uptime of PDU's

- Optionally checks if the default user is still configured.
- Default user is 'root'
- If host 'all' is passed, will iterate over all PDUs

Usage example:
    cookbook sre.pdus.uptime --username MrFoo 'ps1-b5-eqiad.mgmt.eqiad.wmnet'
    cookbook sre.pdus.uptime all
    cookbook sre.pdus.uptime all --check_default
"""

import logging

from requests import Session

from spicerack.interactive import get_secret

from cookbooks.sre.pdus import argument_parser_base, check_default, get_pdu_ips, get_uptime, UptimeError


__title__ = 'List PDU 🔌 uptime'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base()


def run(args, spicerack):
    """Required by Spicerack API."""
    session = Session()
    session.verify = False
    return_code = 0
    current_password = get_secret('Current password')
    session.auth = (args.username, current_password)

    # TODO: check if self.query is a PDU in netbox
    pdus = get_pdu_ips(spicerack.netbox()) if args.query == 'all' else set([args.query])

    for pdu in pdus:
        uptime = None
        try:
            uptime = get_uptime(pdu, session)
            logger.info('%s: uptime %s', pdu, uptime)
        except UptimeError as error:
            logger.error(error)
            return_code = 1
        if args.check_default:
            if check_default(pdu):
                # TODO: delete default user
                return_code = 1
    return return_code
