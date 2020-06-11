"""Reboot PDU's

- Optionally checks if the default user is still configured.
- Default user is 'root'
- If host 'all' is passed, will iterate over all PDUs
- This script does nothing with --dry-run

Usage example:
    cookbook sre.pdus.reboot --username MrFoo 'ps1-b5-eqiad.mgmt.eqiad.wmnet'
    cookbook sre.pdus.reboot all
    cookbook sre.pdus.reboot all --check_default
"""

import logging

from datetime import datetime

from requests import Session
from spicerack.interactive import ensure_shell_is_durable, get_secret

from cookbooks.sre import pdus


__title__ = 'List PDU 🔌 uptime'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return pdus.argument_parser_base()


def run(args, spicerack):
    """Required by Spicerack API."""
    if args.dry_run:
        logger.info('this cookbook does nothing with with --dry-run')
        return 0
    ensure_shell_is_durable()
    session = Session()
    session.verify = False
    return_code = 0
    current_password = get_secret('Current password')
    session.auth = (args.username, current_password)

    # TODO: check if self.query is a PDU in netbox
    _pdus = pdus.get_pdu_ips(spicerack.netbox()) if args.query == 'all' else set([args.query])

    for pdu in _pdus:
        try:
            reboot_time = datetime.utcnow()
            version = pdus.get_version(pdu, session)
            pdus.reboot(pdu, version, session)
            pdus.wait_reboot_since(pdu, reboot_time, session)
        except (pdus.VersionError, pdus.RebootError, pdus.UptimeError) as error:
            logger.error(error)
            return_code = 1
        if args.check_default:
            if pdus.check_default(pdu):
                # TODO: delete default user
                return_code = 1
    return return_code
