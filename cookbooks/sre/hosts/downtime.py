"""Downtime hosts and all their services in Icinga.

- Optionally force a Puppet run on the Icinga host to pick up new hosts or services
- Set Icinga downtime for the given time with a default of 4h if not specified

Usage example:
    cookbook sre.hosts.downtime --days 5 'cp1234*'
    cookbook sre.hosts.downtime --minutes 20 cp1234.eqiad.wmnet
    cookbook sre.hosts.downtime --minutes 20 'O:cache::upload'

"""
import argparse
import logging

from datetime import timedelta

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE


DEFAULT_DOWNTIME_HOURS = 4
__title__ = 'Downtime hosts and all their services in Icinga.'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
    parser.add_argument('-r', '--reason', required=True,
                        help=('The reason for the downtime. The current username and originating host are '
                              'automatically added.'))
    parser.add_argument('-t', '--task-id', help='An optional task ID to refer in the downtime message (i.e. T12345).')
    parser.add_argument('-M', '--minutes', type=int, default=0,
                        help='For how many minutes the downtime should last. [optional, default=0]')
    parser.add_argument('-H', '--hours', type=int, default=0,
                        help='For how many hours the downtime should last. [optional, default=0]')
    parser.add_argument('-D', '--days', type=int, default=0,
                        help='For how many days the downtime should last. [optional, default=0]')
    parser.add_argument('--force-puppet', action='store_true',
                        help='Force a Puppet run on the Icinga host to pick up new hosts or services.')

    return parser


def post_process_args(args):
    """Do any post-processing of the parsed arguments."""
    if not any((args.minutes, args.hours, args.days)):
        logger.info('No downtime length option specified, using default value of %d hours', DEFAULT_DOWNTIME_HOURS)
        args.hours = DEFAULT_DOWNTIME_HOURS


def run(args, spicerack):
    """Required by Spicerack API."""
    post_process_args(args)
    duration = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
    remote_hosts = spicerack.remote().query(args.query)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason(args.reason, task_id=args.task_id)
    phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    if args.force_puppet:
        puppet = spicerack.puppet(spicerack.icinga_master_host)
        logging.info('Forcing a Puppet run on the Icinga server')
        puppet.run(quiet=True, attempts=30)

    logging.info('Downtiming %d hosts and all their services for %s: %s', len(remote_hosts), duration, remote_hosts)
    icinga.downtime_hosts(remote_hosts.hosts, reason, duration=duration)

    if args.task_id is not None:
        message = ('Icinga downtime for {duration} set by {owner} on {n} host(s) and their services '
                   'with reason: {reason}\n```\n{hosts}\n```').format(
            duration=duration, owner=reason.owner, n=len(remote_hosts), reason=args.reason, hosts=remote_hosts)
        phabricator.task_comment(args.task_id, message)
