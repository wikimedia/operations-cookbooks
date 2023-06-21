"""Drain a Ganeti node of running instances"""

import logging

from enum import Enum

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from cookbooks.sre.ganeti import add_location_args
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class Drain(Enum):
    """Enum to represent whether to drain primary instances only or fully"""

    PRIMARY = 1
    FULL = 2


class GanetiDrainNode(CookbookBase):
    """Drain a Ganeti node of running instances

    This cookbook empties a node of running instances. By default only primary instances
    are moved away. This can be used for reboots and similar short term maintenance.

    If a host is going away for a longer time (or if all data will be lost in a reimage),
    the --full option also moves the secondary instances to other nodes.

    By default all Ganeti nodes uses replicate DRBD storage, but for latency-sensitive
    services (currently only needed by etcd) the overhead of DRBD may cause visible
    latency issues. These hosts are stored with local disk storage instead (called "plain").
    If only primary instances are drained, such instances are ignored (since they are
    inherently non-redundant). If a node is fully drained, such instances need to be
    temporarily switched to DRBD using the sre.ganeti.changedisk cookbook first.

    Usage example:
        cookbook sre.ganeti.drain-node --cluster codfw ganeti2022.codfw.wmnet
        cookbook sre.ganeti.drain-node --cluster codfw --full ganeti2022.codfw.wmnet
    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        parser = super().argument_parser()

        add_location_args(parser)
        parser.add_argument('--full', action='store_true', default=False,
                            help='If enabled, also migrate secondary instances')
        parser.add_argument('-t', '--task-id',
                            help='An optional task ID to refer in the downtime message.')
        parser.add_argument('node', help='The FQDN of the Ganeti node to drain.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiDrainNodeRunner(args, self.spicerack)


class GanetiDrainNodeRunner(CookbookRunnerBase):
    """Drain a Ganeti node of running instances"""

    def __init__(self, args, spicerack):
        """Change the disk type of a Ganeti VM."""
        ensure_shell_is_durable()
        self.ganeti = spicerack.ganeti()

        self.rapi = self.ganeti.rapi(args.cluster)
        self.master = spicerack.remote().query(self.rapi.master)

        self.node = args.node
        if args.full:
            self.mode = Drain.FULL
        else:
            self.mode = Drain.PRIMARY
        self.primary_instances = []
        self.secondary_instances = []
        self.plain_instances = []

        if self.node not in spicerack.remote().query('A:ganeti-all').hosts:
            raise RuntimeError(
                f'{self.node} is not a Ganeti server')

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            self.message = f'Draining {self.node} of running VMs'
        else:
            self.phabricator = None

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for draining ganeti node {self.node}'

    def instance_overview(self):
        """Generate/print an overview of running instances."""
        nodes = self.rapi.nodes()
        self.primary_instances = nodes[self.node]['pinst_list']
        self.secondary_instances = nodes[self.node]['sinst_list']

        for instance in self.primary_instances:
            if self.rapi.fetch_instance(instance).get('disk_template') == 'plain':
                self.plain_instances.append(instance)

        logger.info("The following primary instances are running")
        logger.info(self.primary_instances)
        logger.info("The following secondary instances are running")
        logger.info(self.secondary_instances)

    def run_cmd(self, cmd):
        """Run a command on the Ganeti master node and and bail out if missed"""
        try:
            next(self.master.run_sync(cmd))
        except StopIteration as e:
            raise RuntimeError('Failed to run command') from e

    def run(self):
        """Drain a Ganeti node of running instances"""
        self.instance_overview()

        if self.mode == Drain.PRIMARY:
            ask_confirmation(f'Ready to migrate all primary instances away from {self.node}?')

            if len(self.primary_instances) == 0:
                logger.info('No primary instances to migrate, nothing to do')
                return 0

            if set(self.primary_instances) == set(self.plain_instances):
                logger.info('All remaining primary instances are using plain disks, all good')
                return 0

            if self.phabricator is not None:
                self.phabricator.task_comment(self.task_id, self.message)
            self.run_cmd(
                f'gnt-node migrate -f {self.node}')

            self.instance_overview()

        elif self.mode == Drain.FULL:
            ask_confirmation(f'Ready to migrate all secondary instances away from {self.node}?')

            if self.plain_instances:
                logger.info("These instances are using 'plain' disk images")
                logger.info(self.plain_instances)
                logger.info("They first need to be switched to DRBD using the sre.ganeti.changedisk cookbook")
                raise RuntimeError(f'{self.node} cannot by fully drained due to non-DRBD instances.')

            if self.primary_instances:
                logger.info("These primary instances are running, they need to be migrated first")
                logger.info(self.primary_instances)
                raise RuntimeError(f'{self.node} cannot by fully drained due to running primary instances')

            if self.phabricator is not None:
                self.phabricator.task_comment(self.task_id, self.message)
            self.run_cmd(
                f'gnt-node evacuate -s {self.node}')

            self.instance_overview()
        return 0
