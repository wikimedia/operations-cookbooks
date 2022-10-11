"""Change the disk type of a Ganeti VM"""

import logging

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteError
from cookbooks.sre.ganeti import add_location_args
from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class GanetiChangeDisk(CookbookBase):
    """Change the disk type of a Ganeti VM

    By default all Ganeti nodes uses replicate DRBD storage, but for latency-sensitive
    services (currently only needed by etcd) the overhead of DRBD may cause visible
    latency issues. Also, one etcd node can go down any time, so DRBD isn't strictly
    needed either.

    This cookbook changes the storage type for a VM.

    Usage example:
        cookbook sre.ganeti.change-disk --cluster codfw --disktype drbd
                 --secondnode ganeti2023.codfw.wmnet kubetcd2005.codfw.wmnet
        cookbook sre.ganeti.change-disk --cluster codfw --disktype plain kubetcd2005.codfw.wmnet
    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        parser = super().argument_parser()

        add_location_args(parser)
        parser.add_argument('--disktype', choices=['drbd', 'plain'],
                            help='The disk type to switch to', required=True)
        parser.add_argument('--fqdn', help='The FQDN of the Ganeti VM.', required=True)
        parser.add_argument('--secondnode', help='The Ganeti server to become the secondary DRBD node.')
        parser.add_argument('-t', '--task-id',
                            help='An optional task ID to refer in the downtime message.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiChangeDiskRunner(args, self.spicerack)


class GanetiChangeDiskRunner(CookbookRunnerBase):
    """Add a new node to a Ganeti cluster runner"""

    def __init__(self, args, spicerack):
        """Change the disk type of a Ganeti VM."""
        if not args.secondnode and args.disktype == 'drbd':
            raise RuntimeError(
                'If switching to DRBD you need to pass a secondary DRBD node')

        ensure_shell_is_durable()

        self.ganeti = spicerack.ganeti()
        self.remote_vm = spicerack.remote().query(args.fqdn)

        query = f'P{{F:is_virtual = true}} and P{{{args.fqdn}}}'
        try:
            self.remote_vm = spicerack.remote().query(query)
        except RemoteError as e:
            raise RuntimeError(f"the query ({query}) match no hosts") from e

        self.master = spicerack.remote().query(self.ganeti.rapi(args.cluster).master)
        self.alerting_hosts = spicerack.alerting_hosts(self.remote_vm.hosts)
        self.reason = spicerack.admin_reason('Change VM disk type')

        self.rapi = self.ganeti.rapi(args.cluster)
        self.secondnode = args.secondnode
        self.disktype = args.disktype
        self.fqdn = args.fqdn

        initial_disktype = self.rapi.fetch_instance(self.fqdn).get('disk_template')
        if initial_disktype == self.disktype:
            raise RuntimeError(f'{self.fqdn} is already configured for {initial_disktype}.')

        if self.fqdn not in spicerack.remote().query('F:is_virtual = true').hosts:
            raise RuntimeError(
                f'{self.fqdn} is not a Ganeti VM.')

        if args.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
            self.task_id = args.task_id
            self.message = ('VM {vm} switching disk type to {dtype}\n').format(
                vm=self.remote_vm, dtype=self.disktype)
        else:
            self.phabricator = None

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for changing disk type of {self.fqdn} to {self.disktype}'

    def run_cmd(self, cmd, msg):
        """Run a command on the Ganeti master node and and bail out if missed"""
        try:
            next(self.master.run_sync(cmd))

        except StopIteration as e:
            raise RuntimeError(f'{self.fqdn} {msg}. Please fix and re-run the cookbook') from e

    def run(self):
        """Change the disk type of a Ganeti VM."""
        print(f'Ready to switch the disk type of Ganeti VM {self.fqdn} to {self.disktype}?')
        print('Note that during this operation the VM will be powered down temporarily')
        ask_confirmation('Proceed?')

        with self.alerting_hosts.downtimed(self.reason, duration=timedelta(minutes=20)):
            if self.phabricator is not None:
                self.phabricator.task_comment(self.task_id, self.message)

            self.run_cmd(
                f'gnt-instance shutdown {self.fqdn}',
                f'Could not power down {self.fqdn}'
            )

            if self.disktype == 'drbd':
                nodearg = f'-n {self.secondnode}'
            else:
                nodearg = ''

            self.run_cmd(
                f'sudo gnt-instance modify -t {self.disktype} {nodearg} {self.fqdn}',
                f'Failed to switch disk of {self.fqdn} to {self.disktype} storage.'
            )

            self.run_cmd(
                f'gnt-instance startup {self.fqdn}',
                f'Could not start {self.fqdn}'
            )

            if self.rapi.fetch_instance(self.fqdn).get('disk_template') != self.disktype:
                raise RuntimeError(f'{self.fqdn} did not pick up the new disk type.')
