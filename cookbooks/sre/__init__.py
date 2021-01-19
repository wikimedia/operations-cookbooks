"""SRE Cookbooks"""
from abc import abstractmethod, ABCMeta
from argparse import ArgumentParser, Namespace
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import getLogger
from math import ceil
from time import sleep
from typing import List

from cumin import nodeset, NodeSet
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.icinga import IcingaError
from spicerack.remote import RemoteHosts
from wmflib.interactive import (
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
    AbortError,
)


# Shared SRE configuration for phabricator bot
PHABRICATOR_BOT_CONFIG_FILE = '/etc/phabricator_ops-monitoring-bot.conf'
logger = getLogger(__name__)


class ScriptFatalError(AbortError):
    """Custom exception class for errors in reboot pre scripts."""


class ScriptNoneFatalError(AbortError):
    """Custom exception class for errors in reboot pre scripts."""


class RebootPreScriptError(Exception):
    """Custom exception class for errors in reboot pre scripts."""


class RebootPostScriptError(Exception):
    """Custom exception class for errors in reboot post scripts."""


@dataclass
class Results:
    """Class used to manage result reporting."""

    action: str
    hosts: NodeSet
    successful: nodeset = field(default_factory=nodeset)
    failed: NodeSet = field(default_factory=nodeset)

    def fail(self, nodes: NodeSet) -> None:
        """Add nodes to the failed list."""
        unknown_hosts = nodes - self.hosts
        if unknown_hosts:
            ValueError(f'unknown hosts: {unknown_hosts}')
        intersection = self.successful.intersection(nodes)
        if intersection:
            ValueError(f'hosts already recorded successful: {intersection}')
        self.failed.update(nodes)

    def success(self, nodes: NodeSet) -> None:
        """Add nodes to the success list."""
        unknown_hosts = nodes - self.hosts
        if unknown_hosts:
            ValueError(f'unknown hosts: {unknown_hosts}')
        intersection = self.failed.intersection(nodes)
        if intersection:
            ValueError(f'hosts already recorded failed: {intersection}')
        self.successful.update(nodes)

    def report(self) -> int:
        """Report on results."""
        if not self.failed:
            logger.info('All reboots were successful')
            return 0

        logger.info('%s were successful for: %s', self.action, self.successful)
        logger.info('%s failed for: %s', self.action, self.failed)
        logger.info('Check the logs for specific failures')

        leftovers = self.hosts - self.successful - self.failed
        if leftovers:
            logger.info('No action was performed for %s', leftovers)
        return 1


class SREBatchBase(CookbookBase, metaclass=ABCMeta):
    """Common Reboot class CookbookBase class

    By default this get_runner will return an instance of RebootRunner
    """

    batch_default = 1
    batch_max = 40

    def argument_parser(self) -> Namespace:
        """Parse arguments"""
        parser = ArgumentParser(
            description=self.__doc__, formatter_class=ArgparseFormatter
        )

        # Later, specific cookbooks the default alias will be part of the cookbook
        # and the Cumin syntax an optional override

        targets = parser.add_mutually_exclusive_group(required=True)
        targets.add_argument(
            '--alias', '-a', help='A Cumin alias addressing the set of servers'
        )
        targets.add_argument(
            '--query',
            help=('A Cumin query addressing a more narrow set of servers.'
                  ' This parameter requires queries to be formatted using the global grammar'),
        )
        parser.add_argument(
            '--batchsize',
            help='Batch size to act upon',
            type=lambda x: (int(x) <= self.batch_max)
            or parser.error('max batchsize is ' + self.batch_max),
            default=self.batch_default,
        )
        parser.add_argument('--reason', help='Administrative Reason', required=True)
        parser.add_argument('--task-id', help='task id for the change')
        parser.add_argument(
            '--ignore-restart-errors',
            action='store_true',
            help="ignore errors when restarting services",
        )
        parser.add_argument(
            '--grace-sleep',
            type=int,
            help='the amount of time to sleep in seconds between each batch',
        )
        parser.add_argument(
            'action',
            choices=['reboot', 'restart_daemons'],
            help='Choose to reboot the server or restart the daemons related to this cookbook',
        )

        return parser


class SREBatchRunnerBase(CookbookRunnerBase, metaclass=ABCMeta):
    """Reboot Runner Base class preforming generic actions to reboot a batch of hosts

    At the very least children must implement the `allowed_aliases` property to return a list of
    aliases that the specific cookbook is allowed to execute on.

    Implementers may also implement the pre_script and post_script properties to return a list of
    functions to run before and after rebooting the hosts

    The following steps are applied by this runner:

    - Optionally: Run pre action(s) (e.g. depool via conftool or
      sanity check Cassandra cluster state)
    - Set Icinga downtime for all servers in the batch to reboot
    - Reboot
    - Wait for hosts to come back online
    - Optionally: Run post action(s) (e.g. pool via conftool or
      verify that all Cassandra nodes have rejoined the cluster fully)
    - Remove the Icinga downtime after the host has been rebooted, the
      first Puppet run is complete and the (optional) post action has
      return 0
    """

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        ensure_shell_is_durable()
        if args.alias and args.alias not in self.allowed_aliases:
            raise ValueError(
                f"Alias ({args.alias}) does not match allowed aliases: "
                + ', '.join(self.allowed_aliases)
            )
        self._args = args
        self.query = self._query()
        self.hosts = spicerack.remote().query(self.query)
        if not self.hosts:
            raise ValueError(f'Cumin query ({self.query}) matched zero hosts')

        self.number_of_batches = ceil(len(self.hosts.hosts) / args.batchsize)
        self.results = Results(action=args.action, hosts=self.hosts)

        reason = f'{args.action} {self.hosts.hosts}: {args.reason}'
        self.reason = spicerack.admin_reason(reason, args.task_id)
        self.icinga = spicerack.icinga()
        self._spicerack = spicerack
        self.logger = getLogger('.'.join((self.__module__, self.__class__.__name__)))

    @property
    def runtime_description(self) -> str:
        """Required by spicerack api."""
        return f'rolling {self.action} on {self.query}'

    def _query(self) -> str:
        """Return the formatted query"""
        if self._args.query is not None:
            return f'{self._args.query} and {self.allowed_aliases_query}'
        return f'A:{self._args.alias}'

    @property
    def restart_daemons(self) -> List:
        """Property to return a list of daemons to restart"""
        return []

    @property
    @abstractmethod
    def allowed_aliases(self) -> List:
        """Property to return a list of allowed aliases must be implemented in the child"""

    @property
    def allowed_aliases_query(self) -> str:
        """Helper property to return a cumin formatted query of allowed aliases"""
        return '(' + ' or '.join([f'A:{x}' for x in self.allowed_aliases]) + ')'

    @property
    def pre_scripts(self) -> List:
        """Should return a list of scripts to run as prescripts or an empty list"""
        return []

    @property
    def post_scripts(self) -> List:
        """Should return a list of scripts to run as post_scripts or an empty list"""
        return []

    def _restart_daemons(self, hosts: RemoteHosts) -> None:
        """Restart daemons on a set of hosts with downtime

        Arguments:
            hosts (`RemoteHosts`): A list of hosts to action

        """
        systemd_cmd = '/bin/systemdctl'
        if self._args.ignore_restart_errors:
            # Only restart services which are active
            restart_cmds = [
                f'{systemd_cmd} --quiet {daemon} && {systemd_cmd} restart {daemon} || /bin/true'
                for daemon in self.restart_daemons
            ]
        else:
            restart_cmds = [f"{systemd_cmd} restart {' '.join(self.restart_daemons)}"]

        puppet = self._spicerack.puppet(hosts)
        try:
            duration = timedelta(minutes=20)
            with self.icinga.hosts_downtimed(
                hosts.hosts, self.reason, duration=duration
            ):
                now = datetime.utcnow()
                confirm_on_failure(hosts.run_sync, *restart_cmds)
                puppet.run()
                puppet.wait_since(now)
                if not self.icinga.get_status(hosts.hosts).optimal:
                    # TODO: switch to icinga.recheck_failed when available
                    self.icinga.recheck_all_services(hosts.hosts)
                    self.icinga.wait_for_optimal(hosts.hosts)
            self.results.success(hosts.hosts)
        except IcingaError as error:
            ask_confirmation(f'Failed to dowtime hosts: {error}')
            self.logger.warning(error)

        except AbortError as error:
            # Some host failed to come up again, or something fundamental broke.
            # log an error, exit *without* repooling
            self.logger.error(error)
            self.logger.error(
                'Hosts %s have NOT been repooled.', ','.join(self.results.hosts)
            )
            self.results.fail(hosts.hosts)
            raise

    def _reboot(self, hosts: NodeSet) -> None:
        """Reboot a set of hosts with downtime

        Arguments:
            hosts (`NodeSet`): A list of hosts to reboot

        """
        puppet = self._spicerack.puppet(hosts)
        try:
            duration = timedelta(minutes=20)
            with self.icinga.hosts_downtimed(
                hosts.hosts, self.reason, duration=duration
            ):
                reboot_time = datetime.utcnow()
                confirm_on_failure(hosts.reboot, batch_size=len(hosts))
                hosts.wait_reboot_since(reboot_time)
                puppet.wait_since(reboot_time)
                # First let's try to check if icinga is already in optimal state.
                # If not, we require a recheck all service, then
                # wait a grace period before declaring defeat.
                if not self.icinga.get_status(hosts.hosts).optimal:
                    self.icinga.recheck_all_services(hosts.hosts)
                    self.icinga.wait_for_optimal(hosts)
            self.results.success(hosts.hosts)
        except IcingaError as error:
            ask_confirmation(f'Failed to downtime hosts: {error}')
            self.logger.warning(error)

        except AbortError as error:
            # Some host failed to come up again, or something fundamental broke.
            # log an error, continue *without* repooling
            self.logger.error(error)
            self.logger.error(
                'Hosts %s have NOT been repooled.', ','.join(self.results.hosts)
            )
            self.results.fail(hosts.hosts)
            raise

    def _run_scripts(self, scripts: List, hosts: RemoteHosts) -> None:
        """Run a list of scripts

        This function is provided so users can simply populate the pre/post_scripts properties
        to return a list of functions to run.  Each function in the list should accept a
        RemoteHosts argument representing the nodes to execute on. The function should raise
        ScriptFatalError on fatal errors and ScriptNoneFatalError for other errors.

         * if the script raises ScriptFatalError the result is marked as failed and executions halts
         * if the script raises ScriptNoneFatalError the error is logged but execution continues

        Arguments:
            scripts (List): a list of functions to run
            hosts (`RemoteHosts`): a hosts to run the functions on

        Raises:
            ScriptFatalError: if a script has a fatal error

        """
        for script in scripts:
            try:
                confirm_on_failure(script, hosts)
            except ScriptFatalError as e:
                self.results.fail(hosts.hosts)
                self.logger.error('Pre boot script failed, aborting: %s', e)
                raise
            except ScriptNoneFatalError as e:
                self.logger.error('Pre boot script failed, resuming: %s', e)

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Run this function before rebooting the batch of hosts

        By default this function will run:
            self._run_scripts(hosts, self.pre_scripts)

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        self._run_scripts(self.pre_scripts, hosts)

    def action(self, hosts: RemoteHosts) -> None:
        """The main action to preform e.g. reboot, restart a service etc

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        if self._args.action == 'reboot':
            self._reboot(hosts)
        else:
            self._restart_daemons(hosts)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Run this function after preforming the action on the hosts of hosts

        By default this function will run:
            self._run_scripts(hosts, self.post_scripts)

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        self._run_scripts(self.pre_scripts, hosts)

    def batch_action(self) -> None:
        """Cookbook to preform an action on hosts in batches"""
        for batch in self.hosts.split(self.number_of_batches):
            try:
                self.pre_action(batch)
                self.action(batch)
                self.post_action(batch)
                self.results.success(batch)
                sleep(self._args.grace_sleep)
            except Exception as error:  # pylint: disable=broad-except
                # If an exception was raised within the context manager, we have some hosts
                # left depooled, so we stop the loop for human inspection.
                self.results.fail(batch.hosts)
                self.logger.error(
                    'Unrecoverable error. Stopping the rolling reboot: %s', error
                )
                break

        return self.results.report()

    def run(self) -> None:
        """Perform rolling reboot servers in batches"""
        return self.batch_action()
