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

from cookbooks.sre import pdus


__title__ = 'List PDU 🔌 uptime'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return pdus.argument_parser_base()


def run(args, spicerack):
    """Required by Spicerack API."""
    session = Session()
    session.verify = False
    return_code = 0
    current_password = get_secret('Current password')
    session.auth = (args.username, current_password)

    _pdus = pdus.get_pdu_ips(spicerack.netbox(), args.query)

    for pdu in _pdus:
        uptime = None
        try:
            uptime = pdus.get_uptime(pdu, session)
            logger.info('%s: uptime %s', pdu, uptime)
        except pdus.UptimeError as error:
            logger.error(error)
            return_code = 1
        if args.check_default:
            if pdus.check_default(pdu, session):
                # TODO: delete default user
                return_code = 1
    return return_code
