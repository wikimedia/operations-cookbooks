"""Update traffic routing to MediaWiki backends"""
import logging
import re

from spicerack.interactive import ask_confirmation

from cookbooks.sre.switchdc.mediawiki import argument_parser_base, post_process_args, PUPPET_REASON


__title__ = __doc__
ENABLE_COMMAND = 'run-puppet-agent --enable "{message}"'.format(message=PUPPET_REASON)
EXPECTED_DC_TO = r'\+\s+{backend}\.add_backend\(be_{backend}_svc_{dc_to}_wmnet, 100\);'
EXPECTED_DC_FROM = r'\-\s+{backend}\.add_backend\(be_{backend}_svc_{dc_from}_wmnet, 100\);'
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_base(__name__, __title__)


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    logger.info('Update traffic routing to MediaWiki backends from %s to %s', args.dc_from, args.dc_to)

    remote = spicerack.remote()

    logger.info('Running puppet on text caches in %s', args.dc_to)
    remote_output = remote.query(
        'A:cp-text and A:cp-{dc_to} and not A:cp-canary'.format(dc_to=args.dc_to)).run_sync(ENABLE_COMMAND)
    _check_changes(remote_output, EXPECTED_DC_TO, args.dc_from, args.dc_to)

    logger.info('Text caches traffic is now active-active, running puppet in %s', args.dc_from)
    remote_output = remote.query(
        'A:cp-text and A:cp-{dc_from} and not A:cp-canary'.format(dc_from=args.dc_from)).run_sync(ENABLE_COMMAND)
    _check_changes(remote_output, EXPECTED_DC_FROM, args.dc_from, args.dc_to)

    logger.info('Text caches traffic is now active only in %s', args.dc_to)


def _check_changes(remote_output, expected, dc_from, dc_to):
    """Check that the expected changes were applied, ask for ineractive confirmation on failure.

    Arguments:
        remote_output (generator): A cumin.transports.BaseWorker.get_results generator with the output of the Puppet
            run to check for the expected changes.
        expected (str): a regex pattern to use for matching the changes in the Puppet run output.
        dc_from (str): the datacenter we are switching away from.
        dc_to (str): the datacenter we are switching to.

    """
    backends = ('api', 'appservers')
    failed = False

    for nodeset, output in remote_output:
        for backend in backends:
            expected_message = expected.format(backend=backend, dc_from=dc_from, dc_to=dc_to)
            if re.search(expected_message, output.message().decode()) is None:
                failed = True
                logger.error("Unable to verify that message '%s' is in the output of nodeset '%s'",
                             expected_message, str(nodeset))

    if failed:
        ask_confirmation('Please manually verify that the puppet run was applied with the expected changes')
    else:
        logger.info("Expected message '%s' found on all hosts for all backends", expected)
