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
from time import sleep

from requests import Session
from spicerack.interactive import ensure_shell_is_durable, get_secret

from cookbooks.sre import pdus


__title__ = 'List PDU 🔌 uptime'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = pdus.argument_parser_base()
    parser.add_argument('--since', type=int,
                        help='only reboot if the uptime is more then this value in seconds')
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    if spicerack.dry_run:
        logger.info('this cookbook does nothing with with --dry-run')
        return 0
    ensure_shell_is_durable()
    session = Session()
    session.verify = False
    return_code = 0
    current_password = get_secret('Current password')
    session.auth = (args.username, current_password)

    _pdus = pdus.get_pdu_ips(spicerack.netbox(), args.query)

    for pdu in _pdus:
        try:
            if args.since:
                uptime = pdus.parse_uptime(pdus.get_uptime(pdu, session))
                if uptime < args.since:
                    logger.info('%s: Not rebooting uptime is %d', pdu, uptime)
                    continue
            reboot_time = datetime.utcnow()
            version = pdus.get_version(pdu, session)
            pdus.reboot(pdu, version, session)
            # Reboots from expereince take at least 60 seconds
            logger.info('%s: sleep while reboot', pdu)
            sleep(60)
            pdus.wait_reboot_since(pdu, reboot_time, session)
        except (pdus.VersionError, pdus.RebootError, pdus.UptimeError) as error:
            logger.error(error)
            return_code = 1
        if args.check_default:
            if pdus.check_default(pdu, session):
                # TODO: delete default user
                return_code = 1
    return return_code
