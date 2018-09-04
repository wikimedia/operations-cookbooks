"""Restore TTL for various DNS Discovery entries"""
import logging

from cookbooks.sre.switchdc.mediawiki import parse_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def main(args, spicerack):
    """Required by Spicerack API."""
    args = parse_args(__name__, __title__, args)

    records = ('api-rw', 'appservers-rw', 'jobrunner', 'videoscaler')
    logger.info('Restoring DNS Discovery TTL to 300 for records: %s', records)
    dnsdisc_records = spicerack.discovery(*records)
    dnsdisc_records.update_ttl(300)

    logger.info('Removing stale confd files generated in phase 5')
    command = 'rm -fv /var/run/confd-template/.discovery-{{{records}}}.state*.err'.format(records=','.join(records))
    spicerack.remote().query('C:authdns').run_sync(command)
