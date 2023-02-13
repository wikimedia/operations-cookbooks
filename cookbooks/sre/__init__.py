"""SRE Cookbooks"""
from abc import abstractmethod, ABCMeta
from argparse import ArgumentParser, Namespace, SUPPRESS
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from logging import getLogger
from math import ceil
from time import sleep
from typing import List, Union

from cumin import nodeset, NodeSet, nodeset_fromlist
from spicerack import Spicerack
from spicerack.administrative import Reason
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.icinga import IcingaError
from spicerack.remote import RemoteHosts
from wmflib.interactive import (
    ask_confirmation,
    confirm_on_failure,
    ensure_shell_is_durable,
    AbortError,
)


__title__ = __doc__
# Shared SRE configuration for phabricator bot
PHABRICATOR_BOT_CONFIG_FILE = '/etc/phabricator_ops-monitoring-bot.conf'
logger = getLogger(__name__)


class RebootPreScriptError(Exception):
    """Custom exception class for errors in reboot pre scripts."""


class RebootPostScriptError(Exception):
    """Custom exception class for errors in reboot post scripts."""


@dataclass
class Results:
    """Class used to manage result reporting."""

    action: str
    hosts: NodeSet
    successful: NodeSet = field(default_factory=nodeset)
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
            logger.info('All %s were successful', self.action)
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
    grace_sleep = 1
    valid_actions = ('reboot', 'restart_daemons')

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()

        # Later, specific cookbooks the default alias will be part of the cookbook
        # and the Cumin syntax an optional override

        targets = parser.add_mutually_exclusive_group(required=True)
        targets.add_argument(
            '--alias', '-a', help='A Cumin alias addressing the set of servers'
        )
        targets.add_argument(
            '--query',
            help=(
                'A Cumin query addressing a more narrow set of servers.'
                ' This parameter requires queries to be formatted using the global grammar'
            ),
        )
        parser.add_argument(
            '--batchsize',
            help='Batch size to act upon',
            type=int, choices=range(1, self.batch_max + 1), metavar=f'[1-{self.batch_max}]',
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
            default=self.grace_sleep,
            help='the amount of time to sleep in seconds between each batch',
        )
        act = parser.add_argument(
            'action',
            choices=self.valid_actions,
            help='Choose to reboot the server or restart the daemons related to this cookbook',
        )
        # if we only have one action available, we might as well assume it's the default.
        if len(self.valid_actions) == 1:
            act.default = self.valid_actions[0]
            act.help = SUPPRESS
            act.required = False

        return parser


class SREBatchRunnerBase(CookbookRunnerBase, metaclass=ABCMeta):
    """Reboot Runner Base class performing generic actions to reboot a batch of hosts

    At the very least children must implement the `allowed_aliases` property to return a list of
    aliases that the specific cookbook is allowed to execute on.

    Implementers may also implement the pre_script and post_script properties to return a list of
    functions to run before and after rebooting the hosts

    The following steps are applied by this runner:

    - Optionally: Run pre action(s) (e.g. depool via conftool or
      sanity check Cassandra cluster state)
    - Set Icinga/Alertmanager downtime for all servers in the batch to reboot
    - Reboot/Restart services
    - If reboot: Wait for hosts to come back online
    - If reboot: Wait for the first puppet run
    - Wait for Icinga optimal status
    - Optionally: Run post action(s) (e.g. pool via conftool or
      verify that all Cassandra nodes have rejoined the cluster fully)
    - Remove the Icinga/Alertmanager downtime

    """

    disable_puppet_on_restart = False

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        ensure_shell_is_durable()
        if args.alias and args.alias not in self.allowed_aliases:
            raise ValueError(
                f"Alias ({args.alias}) does not match allowed aliases: "
                + ', '.join(self.allowed_aliases)
            )
        self._args = args
        self._spicerack = spicerack
        self.logger = getLogger('.'.join((self.__module__, self.__class__.__name__)))
        self.host_groups = self._hosts()
        self.all_hosts = nodeset_fromlist(group.hosts for group in self.host_groups)
        self.results = Results(action=args.action, hosts=self.all_hosts)

    def _reason(self, hosts: NodeSet) -> Reason:
        """Return the reason for administrative actions on given hosts"""
        reason = f'{self._args.action} {hosts}: {self._args.reason}'
        return self._spicerack.admin_reason(reason, self._args.task_id)

    @property
    def runtime_description(self) -> str:
        """Required by spicerack api."""
        return f'rolling {self._args.action} on {self._query()}'

    def _query(self) -> str:
        """Return the formatted query"""
        if self._args.query is not None:
            return f'{self._args.query} and {self.allowed_aliases_query}'
        return f'A:{self._args.alias}'

    def _hosts(self) -> List[RemoteHosts]:
        """Return a list of RemoteHosts to sequentially operate on"""
        query = self._query()
        self.logger.debug('Effective remote query is: %s', query)
        hosts = self._spicerack.remote().query(query)
        if not hosts:
            raise ValueError(f'Cumin query ({query}) matched zero hosts')
        return [hosts]

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

    def _batchsize(self, number_of_hosts: int) -> int:
        """Should return the batch size to use

        Arguments:
            number_of_hosts (`int`): Number of hosts in current host group

        """
        # pylint: disable=unused-argument
        return self._args.batchsize

    def _sleep(self, seconds: Union[int, float]) -> None:
        """A DRY-RUN aware version of time.sleep()."""
        if self._spicerack.dry_run:
            self.logger.info('Would have slept for %s seconds', seconds)
        else:
            self.logger.info('Sleeping for %s seconds', seconds)
            sleep(seconds)

    def _restart_daemons(self, hosts: RemoteHosts) -> None:
        """Restart daemons on a set of hosts with downtime

        Arguments:
            hosts (`RemoteHosts`): A list of hosts to action

        """
        systemd_cmd = '/bin/systemctl'
        if self._args.ignore_restart_errors:
            # Only restart services which are active
            restart_cmds = [
                f'{systemd_cmd} --quiet is-active {daemon} && {systemd_cmd} restart {daemon} || /bin/true'
                for daemon in self.restart_daemons
            ]
        else:
            restart_cmds = [f"{systemd_cmd} restart {' '.join(self.restart_daemons)}"]

        reason = self._reason(hosts.hosts)
        puppet = self._spicerack.puppet(hosts.hosts)
        icinga_hosts = self._spicerack.icinga_hosts(hosts.hosts)
        alerting_hosts = self._spicerack.alerting_hosts(hosts.hosts)
        try:
            duration = timedelta(minutes=20)
            with alerting_hosts.downtimed(reason, duration=duration):
                if self.disable_puppet_on_restart:
                    with puppet.disabled(reason):
                        confirm_on_failure(hosts.run_sync, *restart_cmds)
                else:
                    confirm_on_failure(hosts.run_sync, *restart_cmds)
                icinga_hosts.wait_for_optimal()
            self.results.success(hosts.hosts)
        except IcingaError as error:
            ask_confirmation(f'Failed to downtime hosts: {error}')
            self.logger.warning(error)

        except AbortError as error:
            # Some host failed to come up again, or something fundamental broke.
            # log an error, exit *without* repooling
            self.logger.error(error)
            self.logger.error(
                'Error restarting daemons on: Hosts %s, they may still be depooled',
                hosts,
            )
            self.results.fail(hosts.hosts)
            raise

    def _reboot(self, hosts: RemoteHosts) -> None:
        """Reboot a set of hosts with downtime

        Arguments:
            hosts (`NodeSet`): A list of hosts to reboot

        """
        reason = self._reason(hosts)
        puppet = self._spicerack.puppet(hosts)
        icinga_hosts = self._spicerack.icinga_hosts(hosts.hosts)
        alerting_hosts = self._spicerack.alerting_hosts(hosts.hosts)
        try:
            duration = timedelta(minutes=20)
            with alerting_hosts.downtimed(reason, duration=duration):
                reboot_time = datetime.utcnow()
                confirm_on_failure(hosts.reboot, batch_size=len(hosts))
                # Avoid exceptions in dry_run mode:
                # * "Uptime higher than threshold"
                # * "Successful Puppet run too old"
                if not self._spicerack.dry_run:
                    hosts.wait_reboot_since(reboot_time, print_progress_bars=False)
                    puppet.wait_since(reboot_time)
                icinga_hosts.wait_for_optimal()
            self.results.success(hosts.hosts)
        except IcingaError as error:
            ask_confirmation(f'Failed to downtime hosts: {error}')
            self.logger.warning(error)

        except AbortError as error:
            # Some host failed to come up again, or something fundamental broke.
            # log an error, continue *without* repooling
            self.logger.error(error)
            self.logger.error(
                'Error rebooting: Hosts %s, they may still be depooled', hosts
            )
            self.results.fail(hosts.hosts)
            raise

    def _run_scripts(self, scripts: List, hosts: RemoteHosts) -> None:
        """Run a list of scripts

        This function is provided so users can simply populate the pre/post_scripts properties
        to return a list of scripts to run.  Each script in the list should return with:
          * 0 on success
          * any other return value is considered an error

        Arguments:
            scripts (List): a list of scripts to run (the script must exist on the host)
            hosts (`RemoteHosts`): hosts to run the scripts on

        Raises:
            AbortError: if a script has an error and the user choose to abort

        """
        for script in scripts:
            try:
                confirm_on_failure(hosts.run_async, script)
            except AbortError:
                self.logger.error('%s: execution aborted', script)
                self.results.fail(hosts.hosts)
                raise

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Run this function before performing the action on the batch of hosts

        By default this function will run:
            self._run_scripts(hosts, self.pre_scripts)

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        self._run_scripts(self.pre_scripts, hosts)

    def action(self, hosts: RemoteHosts) -> None:
        """The main action to perform e.g. reboot, restart a service etc

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        if self._args.action == 'reboot':
            self._reboot(hosts)
        elif self._args.action == 'restart_daemons':
            self._restart_daemons(hosts)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Run this function after performing the action on the batch of hosts

        By default this function will run:
            self._run_scripts(hosts, self.post_scripts)

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        self._run_scripts(self.post_scripts, hosts)

    def group_action(self, host_group_idx, number_of_batches: int) -> None:
        """Action to perform once for every host group, right before working on the first batch

        Arguments:
            host_group_idx (`int`): the index of the current host group in self.host_groups
            number_of_batches (`int`): the total number of batches in this host group

        """

    def batch_action(self) -> None:
        """Cookbook to perform an action on all hosts per group in batches"""
        for host_group_idx, host_group in enumerate(self.host_groups):
            number_of_hosts = len(host_group.hosts)
            number_of_batches = ceil(number_of_hosts / self._batchsize(number_of_hosts))
            self.group_action(host_group_idx, number_of_batches)
            for batch in host_group.split(number_of_batches):
                try:
                    self.pre_action(batch)
                    self.action(batch)
                    self.post_action(batch)
                    self._sleep(self._args.grace_sleep)
                    self.results.success(batch.hosts)
                except Exception as error:  # pylint: disable=broad-except
                    # If an exception was raised within the context manager, we have some hosts
                    # left depooled, so we stop the loop for human inspection.
                    self.results.fail(batch.hosts)
                    self.logger.error(
                        'Unrecoverable error. Stopping the rolling %s: %s',
                        self._args.action,
                        error,
                    )
                    break

        return self.results.report()

    def run(self) -> None:
        """Perform rolling reboot servers in batches"""
        return self.batch_action()


class SRELBBatchRunnerBase(SREBatchRunnerBase, metaclass=ABCMeta):
    """SRE batch runnner which is aware of conftool pool state"""

    depool_threshold = 1
    depool_sleep = 5
    repool_sleep = 5

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        if args.batchsize > self.depool_threshold:
            raise ValueError(
                f"batchsize (args.batchsize) can't be greater then the depool_threshold {self.depool_threshold}"
            )
        # TODO: check currently depooled hosts + batchsize is less then depool_threshold
        self._confctl = spicerack.confctl('node')
        super().__init__(args, spicerack)

    @property
    def depool_services(self) -> List[str]:
        """Property to return a list of specific services to depool/repool. If empty means all services."""
        return []

    def wait_for_depool(self):
        """Perform action to check a host has been de-pooled.

        By default this function just sleeps for `depool_sleep` seconds

        """
        self._sleep(self.depool_sleep)

    def wait_for_repool(self):
        """Perform action to check a host is ready to be repooled.

        By default this function just sleeps for `repool_sleep` seconds

        """
        self._sleep(self.repool_sleep)

    def action(self, hosts: RemoteHosts) -> None:
        """The main action to perform e.g. reboot, restart a service etc

        Arguments:
            hosts (`RemoteHosts`): a list of functions to run

        """
        kwargs = {}
        depool_services = "|".join(self.depool_services)
        if depool_services:
            kwargs["service"] = depool_services

        try:
            with self._confctl.change_and_revert(
                'pooled', 'yes', 'no', name="|".join(hosts.hosts.striter()), **kwargs
            ):
                self.wait_for_depool()
                super().action(hosts)
                self.wait_for_repool()
        except Exception:
            self.logger.error('#' * 50)
            self.logger.error(
                'Unrecoverable error the following hosts are still depooled: %s', hosts
            )
            self.logger.error('#' * 50)
            raise
