"""Switch MediaWiki active datacenter"""
import logging

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args


__title__ = __doc__
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Switch MediaWiki active datacenter to %s', args.dc_to)

    records = ('api-rw', 'appservers-rw', 'jobrunner', 'videoscaler')
    dnsdisc_records = spicerack.discovery(*records)
    mediawiki = spicerack.mediawiki()

    # Pool DNS discovery records on the new dc.
    # This will NOT trigger confd to change the DNS admin state as it will cause a validation error
    dnsdisc_records.pool(args.dc_to)

    # Switch MediaWiki master datacenter
    mediawiki.set_master_datacenter(args.dc_to)

    # Depool DNS discovery records on the old dc, confd will apply the change
    dnsdisc_records.depool(args.dc_from)

    # Verify that the IP of the records matches the expected one
    for record in records:
        name = record.replace('-rw', '')
        dnsdisc_records.check_record(record, '{name}.svc.{dc_to}.wmnet'.format(name=name, dc_to=args.dc_to))
