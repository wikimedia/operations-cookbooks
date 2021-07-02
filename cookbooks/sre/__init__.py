"""SRE Cookbooks"""
import argparse
import logging
import time
import math

from datetime import datetime, timedelta
from typing import List
from collections import namedtuple

import attr

from cumin import NodeSet
from spicerack.icinga import IcingaError
from spicerack.puppet import PuppetHostsCheckError
from spicerack.remote import RemoteCheckError, RemoteExecutionError


__title__ = __doc__
PHABRICATOR_BOT_CONFIG_FILE = '/etc/phabricator_ops-monitoring-bot.conf'
GRACE_SLEEP = 10
ScriptReturn = namedtuple('ScriptReturn', 'returncode output')
logger = logging.getLogger(__name__)


# TODO: Move the various bits to Spicerack
#
# The following functions allow to downtime a group of servers and reboot them
#
# Create a common "library cookbook" to be used by more specialised
# cookbooks which only create the service-specific settings:
#
# - The applicable Cumin aliases
# - The amount of servers to reboot per batch
# - An optional list of pre and post action
#
# The following steps are applied:
#
# - Address servers by Cumin globbing or Cumin alias
# - Cumin alias by default, globbing can manually override
# - Optionally: Run pre action(s) (e.g. depool via conftool or
#   sanity check Cassandra cluster state)
# - Set Icinga downtime for all servers in the batch to reboot
# - Reboot
# - Wait for hosts to come back online
# - Remove the Icinga downtime after the host has been rebooted, the
#   first Puppet run is complete and the (optional) post action has
#   return 0
# - Optionally: Run post action(s) (e.g. repool via conftool or
#   verify that all Cassandra nodes have rejoined the cluster fully)

# Silence some CI sillyness
# pylint: disable-msg=too-many-arguments
# pylint: disable-msg=too-many-locals
# pylint: disable-msg=broad-except


class RebootPreScriptError(Exception):
    """Custom exception class for errors in reboot pre scripts."""


class RebootPostScriptError(Exception):
    """Custom exception class for errors in reboot post scripts."""


def argument_parser_reboot_groups(batch_default):
    """Parse arguments"""
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)

    # Later, specific cookbooks the default alias will be part of the cookbook
    # and the Cumin syntax an optional override

    targets = parser.add_mutually_exclusive_group(required=True)
    targets.add_argument('--alias', '-a', help='A Cumin alias addressing the set of servers')
    targets.add_argument('--raw', help='A Cumin query addressing a more narrow set of servers')
    parser.add_argument('--batchsize', help='Batch size to act upon',
                        type=int, default=batch_default)
    return parser


@attr.s
class Results:
    """Class used to manage result reporting."""

    hosts: List[str] = attr.ib()
    successful: List[str] = attr.ib(factory=list)
    failed: List[str] = attr.ib(factory=list)

    def fail(self, nodes: NodeSet):
        """Add nodes to the failed list."""
        self.failed.extend(nodes.striter())

    def success(self, nodes: NodeSet):
        """Add nodes to the success list."""
        self.successful.extend(nodes.striter())

    def report(self):
        """Report on results."""
        if not self.failed:
            logger.info('All reboots were successful')
            return 0

        logger.info('Reboots were successful for: %s', ','.join(self.successful))
        logger.info('Groups with failed reboots: %s', len(self.failed))
        logger.info('Hosts in those groups: %s', ','.join(self.failed))
        logger.info('Check the logs for specific failures')
        leftovers = list(set(self.hosts) - set(self.successful) - set(self.failed))
        if leftovers:
            logger.info('No action was performed for %s', ','.join(leftovers))
        return 1


def reboot_set(spicerack, hosts_set, results):
    """Reboot a set of hosts with downtime"""
    icinga_hosts = spicerack.icinga_hosts(hosts_set.hosts)
    puppet = spicerack.puppet(hosts_set)
    reason = spicerack.admin_reason('Rebooting {}'.format(hosts_set))
    try:
        with icinga_hosts.downtimed(reason, duration=timedelta(minutes=20)):
            reboot_time = datetime.utcnow()
            hosts_set.reboot(batch_size=len(hosts_set))
            hosts_set.wait_reboot_since(reboot_time)
            puppet.wait_since(reboot_time)
            # First let's try to check if icinga is already in optimal state.
            # If not, we require a recheck all services, then
            # wait a grace period before declaring defeat.
            if not icinga_hosts.get_status().optimal:
                icinga_hosts.recheck_all_services()
                icinga_hosts.wait_for_optimal()
        results.success(hosts_set.hosts)
    except IcingaError as e:
        logger.warning(e)

    except (PuppetHostsCheckError, RemoteCheckError, RemoteExecutionError) as e:
        # Some host failed to come up again, or something fundamental broke.
        # log an error, exit *without* repooling
        logger.error(e)
        logger.error('Hosts %s have NOT been repooled.', ','.join(results.hosts))
        results.fail(hosts_set.hosts)
        raise


def reboot_group(spicerack, raw, alias, allowed_aliases, batchsize, pre_scripts, post_scripts):
    """Cookbook to reboot hosts

    Arguments:
    spicerack       : The spicerack remote
    raw             : A raw Cumin query in Puppetdb grammar targettting a set of servers (needs
                      to match the list of allowed Cumin aliases)
    alias           : A Cumin alias of servers to reboot (needs to match the list of allowed
                      Cumin aliases)
    allowed_aliases : A list of Cumin aliases on which the cookbook can be run
    batchsize       : An integer value which specifies how many servers can be rebooted at once
    pre_scripts     : List of functions, which are expected to return the following
                       * 0 on success
                       * 1 on a fatal error (aborts further reboots)
                       * 2 on a soft error (log, but don't abort, e.g. something to investigate)
                       Each pre and post scripts receive a Cumin host list as first argument
    post_scripts    : See above

    """
    # If the user has specified a raw Cumin query (e.g. to only reboot some servers of an alias)
    if raw:
        cumin_query = 'P{} and ({})'.format(
            raw,
            ' or '.join(["A:{}".format(x) for x in allowed_aliases]))
    else:
        if alias not in allowed_aliases:
            raise ValueError("Alias ({} does not match allowed aliases: {}".format(
                alias, ', '.join(allowed_aliases)))
        cumin_query = 'A:{}'.format(alias)

    total_hosts = spicerack.remote().query(cumin_query)
    if not total_hosts:
        raise ValueError("Cumin query matched zero hosts")

    results = Results(hosts=total_hosts)
    n_batches = math.ceil(len(total_hosts) / batchsize)

    for batch in total_hosts.split(n_batches):
        try:
            time.sleep(GRACE_SLEEP)
            if pre_scripts:
                for script in pre_scripts:
                    script_result = script.function(batch)
                    if script_result.returncode == 1:
                        results.fail(batch.hosts)
                        logger.error('Pre boot script failed, aborting: %s',
                                     script_result.output)
                        raise RebootPreScriptError(script_result.output)
                    if script_result.returncode == 2:
                        logger.error('Pre boot script failed, resuming: %s',
                                     script_result.output)

            reboot_set(spicerack, batch, results)

            if post_scripts:
                for script in post_scripts:
                    script_result = script.function(batch)
                    if script_result.returncode == 1:
                        results.fail(batch.hosts)
                        logger.error('Post boot script failed, aborting: %s',
                                     script_result.output)
                        raise RebootPostScriptError(script_result.output)
                    if script_result.returncode == 2:
                        logger.error('Post boot script failed, resuming: %s',
                                     script_result.output)

            results.success(batch)

        except Exception as e:
            # If an exception was raised within the context manager, we have some hosts
            # left depooled, so we stop the loop for human inspection.
            results.fail(batch.hosts)
            logger.error('Unrecoverable error. Stopping the rolling reboot: %s', e)
            break

    return results.report()
