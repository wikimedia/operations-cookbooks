"""Reset the IPMI password for hosts

- the current password and the new password will be requested to avoid passwords remaining in bash history

Usage example:
    cookbook sre.hosts.ipmi_password_reset 'cp1234*'
    cookbook sre.hosts.ipmi_password_reset cp1234.eqiad.wmnet
    cookbook sre.hosts.ipmi_password_reset 'O:cache::upload'

"""
import logging
import os

from argparse import ArgumentParser, RawDescriptionHelpFormatter
from getpass import getpass

from cumin import NodeSet
from wmflib.interactive import ensure_shell_is_durable

from spicerack.interactive import get_management_password
from spicerack.ipmi import IPMI_PASSWORD_MIN_LEN, IPMI_PASSWORD_MAX_LEN, IpmiError, IpmiCheckError
from spicerack.netbox import NetboxError


__owner_team__ = 'Infrastructure Foundations'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = ArgumentParser(
        description=__doc__,
        formatter_class=RawDescriptionHelpFormatter
    )
    parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
    parser.add_argument(
        '-t', '--task-id',
        help='An optional task ID to update, also used in log messages (i.e. T12345).'
    )
    parser.add_argument(
        '--new',
        action='store_true',
        help=('For new hardware not yet in PuppetDB, used in conjunction with a query of the form D{FQDN1,FQDN2}.')
    )
    return parser


def run(args, spicerack):
    """Required by Spicerack API."""
    ensure_shell_is_durable()
    remote = spicerack.remote()
    if args.new:
        remote_hosts = remote.query(args.query).hosts
    else:
        # start by collecting all physical hosts
        remote_hosts = remote.query('F:virtual = physical').hosts
        # get the hosts that intersect with the user query
        remote_hosts &= remote.query(args.query).hosts

    if not remote_hosts:
        logger.info('No hosts selected, bailing out.')
        return 1

    reason = spicerack.admin_reason(
        'Updating IPMI password on {} hosts'.format(len(remote_hosts)),
        task_id=args.task_id
    )
    os.environ['MGMT_PASSWORD'] = get_management_password()

    while True:
        new_password = getpass(prompt='New Management Password: ')
        # TODO:
        # - in the ipmi module remove the use of the env variable and use
        #   subprocess.run() with the env set.
        # - add this logic to the interactive.get_management_password(),
        #   add a parameter to tweak the prompt text between existing and new
        # - use that function here directly as it will not anymore set the env variable
        if len(new_password) < IPMI_PASSWORD_MIN_LEN and \
                len(new_password) > IPMI_PASSWORD_MAX_LEN:
            logger.error(
                'password must between %s and %s bytes long',
                IPMI_PASSWORD_MIN_LEN,
                IPMI_PASSWORD_MAX_LEN
            )
            continue
        repeat_password = getpass(prompt='Retype New Management Password: ')
        if repeat_password != new_password:
            logger.error('both passwords must match')
            continue
        break

    spicerack.sal_logger.info(reason)
    host_status = {'success': NodeSet(), 'fail_netbox': NodeSet(), 'fail_ipmi': NodeSet()}
    for host in remote_hosts:
        try:
            mgmt_host = spicerack.netbox_server(host.split('.')[0]).mgmt_fqdn
        except NetboxError as error:
            logger.warning('unable to get the mgmt address from Netbox for %s: %s', host, error)
            host_status['fail_netbox'].add(host)
            continue

        ipmi = spicerack.ipmi(mgmt_host)
        try:
            ipmi.reset_password('root', new_password)
        except (IpmiCheckError, IpmiError) as error:
            logger.error('IPMI error encountered %s: %s', host, error)
            host_status['fail_ipmi'].add(host)
            continue
        logger.info('password updated successfully for: %s', host)
        host_status['success'].add(host)
    message = '''
    The following hosts completed successfully:
        {}

    The following hosts were unable to get the management address from Netbox:
        {}

    The following hosts had ipmi failures:
        {}
        '''.format(host_status['success'], host_status['fail_netbox'], host_status['fail_ipmi'])
    logger.info(message)
    return 0
