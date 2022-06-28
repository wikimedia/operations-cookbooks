"""Add a new node to a Ganeti cluster"""

import argparse
import logging

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError
from cookbooks.sre.ganeti import add_location_args

logger = logging.getLogger(__name__)


class GanetiAddNode(CookbookBase):
    """Add a new node to a Ganeti cluster

    Validate various preconditions which need to happen to add a new node to
    a Ganeti cluster and eventually add it.

    Usage example:
        cookbook sre.ganeti.addnode --cluster eqiad --group row_A ganeti5004.eqsin.wmnet
    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=ArgparseFormatter)

        add_location_args(parser)
        parser.add_argument('fqdn', help='The FQDN of the new Ganeti node.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiAddNodeRunner(args, self.spicerack)


class GanetiAddNodeRunner(CookbookRunnerBase):
    """Add a new node to a Ganeti cluster runner"""

    def __init__(self, args, spicerack):
        """Add a new node to a Ganeti cluster."""
        ganeti = spicerack.ganeti()
        # Validate cluster and group names, will raise if they're not correct.
        ganeti.get_group(args.group, cluster=args.cluster)
        self.remote = spicerack.remote()
        self.master = self.remote.query(ganeti.rapi(args.cluster).master)
        self.remote_host = self.remote.query(args.fqdn)

        self.cluster = args.cluster
        self.group = args.group
        self.fqdn = args.fqdn
        ensure_shell_is_durable()

        if len(self.remote_host) == 0:
            raise RuntimeError('Specified server not found, bailing out')

        if len(self.remote_host) != 1:
            raise RuntimeError('Only a single server can be added at a time')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for new host {self.fqdn} to cluster {self.cluster} and group {self.group}'

    def validate_state(self, cmd, msg, *, run_on_masternode=False):
        """Ensure a given precondition for adding a Ganeti node and bail out if missed"""
        try:
            if run_on_masternode:
                status = next(self.master.run_sync(cmd))
            else:
                status = next(self.remote_host.run_sync(cmd))

        except StopIteration:
            status = None

        if not status:
            raise RuntimeError(f'{self.fqdn} {msg}. Please fix and re-run the cookbook')

    def is_valid_bridge(self, bridge):
        """Ensure a that a bridge interface is correctly configured on the switches"""
        self.validate_state(f'ip -br link show type bridge dev {bridge}', f'No {bridge} bridge configured')

        cmd = f'ip -br link show master {bridge} '
        cmd += "| awk '!/tap/{print $1}' | awk -F'@' '{print $1}'"
        try:
            result = self.remote_host.run_sync(cmd)
            for _, output in result:
                interface = output.message().decode()

        except StopIteration:
            interface = None

        if not interface:
            raise RuntimeError(
                f'{self.fqdn} Could not detect interface for bridge {bridge}. Please fix and re-run the cookbook')

        valid_bridge = False
        cmd = f'bridge fdb show br {bridge} dev {interface} | grep -vc permanent'
        try:
            result = self.remote_host.run_sync(cmd)
            for _, output in result:
                bridge_check = output.message().decode()
            if bridge_check != "0":
                valid_bridge = True

        except (StopIteration, RemoteExecutionError):
            valid_bridge = False

        if not valid_bridge:
            raise RuntimeError(
                f'Switch is not trunking the correct VLANs for the {bridge} bridge. Enable them in Netbox')

    def run(self):
        """Add a new node to a Ganeti cluster."""
        print(f'Ready to add Ganeti node {self.fqdn} in the {self.cluster} cluster')
        ask_confirmation('Is this correct?')

        if self.fqdn not in self.remote.query('A:ganeti-all').hosts:
            raise RuntimeError(
                f'{self.fqdn} does have not have the Ganeti role applied. Please fix and re-run the cookbook')

        self.validate_state(
            'ls /dev/kvm',
            'does have not have virtualisation enabled in BIOS'
        )

        self.validate_state(
            'vgs | grep "ganeti "',
            ('No "ganeti" volume group found. You need to remove the swap device on /dev/md2, '
             'create a PV on /dev/md2 and eventually create a VG named "ganeti". Make sure to '
             'remove the stale swap entry from fstab as well'),
        )

        self.validate_state(
            f'grep {self.fqdn} /etc/ferm/conf.d/10_ganeti_ssh_cluster',
            ('The node cannot be found in the Ferm config of the Ganeti master.'
             'Make sure to add it to the profile::ganeti::nodes Hiera config.'),
            run_on_masternode=True,
        )

        self.is_valid_bridge('private')
        self.is_valid_bridge('public')

        if self.fqdn in self.remote.query('A:eqiad').hosts:
            self.is_valid_bridge('analytics')

        self.master.run_sync(f'gnt-node add --no-ssh-key-check -g "{self.group}" "{self.fqdn}"')
        ask_confirmation('Has the node been added correctly?')

        self.master.run_sync('gnt-cluster verify')
        ask_confirmation('Verify that the cluster state looks correct.')

        self.master.run_sync('gnt-cluster verify-disks')
        ask_confirmation('Verify that the disk state looks correct.')
