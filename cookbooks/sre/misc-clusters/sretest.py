"""sretest reboot cookbook

Usage example:
    cookbook sre.misc-clusters.sretest

"""

import logging
from spicerack.remote import RemoteExecutionError
from collections import namedtuple
from cookbooks.sre.hosts.reboot_groups import argument_parser_reboot_groups, reboot_group

ScriptReturn = namedtuple('ScriptReturn', 'returncode output')

__title__ = 'SREtest reboot cookbook'
batch_size = 1
allowed_aliases = ['sretest']

logger = logging.getLogger(__name__)


def pre_action_example(hosts):
    """Run pre boot script"""
    actions = ['echo "Nothing really happens"', 'echo "Really"']

    try:
        hosts.run_sync(*actions)
    except RemoteExecutionError as e:
        logger.error("Failed to run pre reboot script:")
        logger.error(e)
        return ScriptReturn(1, e)

    return ScriptReturn(0, "")


def argument_parser():
    """As specified by Spicerack API."""
    return argument_parser_reboot_groups(__name__, batch_size)


def run(args, spicerack):
    """Required by Spicerack API."""
    logger.info('Running reboot cookbook for %s', __title__)

    return reboot_group(spicerack, args.query, args.alias, allowed_aliases, args.batchsize,
                        [pre_action_example], None)
