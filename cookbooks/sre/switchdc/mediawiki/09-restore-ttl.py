"""Restore TTL for various DNS Discovery entries"""
import logging

from spicerack.remote import RemoteExecutionError

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, MEDIAWIKI_SERVICES, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)

    logger.info('Restoring DNS Discovery TTL to 300 for records: %s', MEDIAWIKI_SERVICES)
    dnsdisc_records = spicerack.discovery(*MEDIAWIKI_SERVICES)
    dnsdisc_records.update_ttl(300)

    logger.info('Removing stale confd files generated when switching discovery records')
    command = 'rm -fv /var/run/confd-template/.discovery-{{{records}}}.state*.err'.format(
        records=','.join(MEDIAWIKI_SERVICES))

    # As authdns hosts could be depooled and under maintenance but still receiving confd updates and hence
    # generating the error files, attempt to delete them best-effort, just logging in case of failure.
    try:
        spicerack.remote().query('A:dns-auth').run_sync(command)
    except RemoteExecutionError:
        logger.warning(
            "Confd templates error files not properly cleared, check the output above for failures. "
            "Check if any dnsauth host was unreachable or under maintenance."
        )
