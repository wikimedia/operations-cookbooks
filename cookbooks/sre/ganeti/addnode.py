"""Add a new node to a Ganeti cluster"""

import argparse
import logging

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from cookbooks.sre.ganeti import get_locations


logger = logging.getLogger(__name__)


class GanetiAddNode(CookbookBase):
    """Add a new node to a Ganeti cluster

    Validate various preconditions which need to happen to add a new node to
    a Ganeti cluster and eventually add it.

    Usage example:
        cookbook sre.ganeti.addnode eqsin ganeti5004.eqsin.wmnet
    """

    def argument_parser(self):
        """Parse command-line arguments for this module per spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__,
                                         formatter_class=ArgparseFormatter)

        parser.add_argument('location', choices=sorted(get_locations().keys()),
                            help='The Ganeti cluster to which the new node should be added.')
        parser.add_argument('fqdn', help='The FQDN of the new Ganeti node.')
        parser.add_argument('group', help='The Ganeti group to add the node to.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return GanetiAddNodeRunner(args, self.spicerack)


class GanetiAddNodeRunner(CookbookRunnerBase):
    """Add a new node to a Ganeti cluster runner"""

    def __init__(self, args, spicerack):
        """Add a new node to a Ganeti cluster."""
        self.cluster, self.row, self.datacenter = get_locations()[args.location]
        ganeti = spicerack.ganeti()
        self.remote = spicerack.remote()
        self.master = self.remote.query(ganeti.rapi(self.cluster).master)
        self.remote_host = self.remote.query(args.fqdn)
        self.fqdn = args.fqdn
        self.group = args.group

        ensure_shell_is_durable()

        if len(self.remote_host) == 0:
            raise RuntimeError('Specified server not found, bailing out')

        if len(self.remote_host) != 1:
            raise RuntimeError('Only a single server can be added at a time')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for new host {} to {}'.format(self.fqdn, self.cluster)

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
            raise RuntimeError(
                '{} {}. Please fix and re-run the cookbook'.format(self.fqdn, msg)
            )

    def is_valid_bridge(self, bridge):
        """Ensure a that a bridge interface is correctly configured on the switches"""
        self.validate_state(
            'ip -br link show type bridge dev {}'.format(bridge),
            'No {} bridge configured'.format(bridge),
        )

        cmd = "ip -br link show master {bridge} ".format(bridge=bridge)
        cmd += "| awk '!/tap/{print $1}' | awk -F'@' '{print $1}'"
        try:
            result = self.remote_host.run_sync(cmd)
            for _, output in result:
                interface = output.message().decode()

        except StopIteration:
            interface = None

        if not interface:
            raise RuntimeError(
                '{} Could not detect interface for bridge {}. Please fix and re-run the cookbook'
                .format(self.fqdn, bridge)
            )

        valid = True
        cmd = "bridge fdb show br {} dev {} | grep -vc permanent".format(bridge, interface)
        try:
            result = self.remote_host.run_sync(cmd)
            for _, output in result:
                bridge_check = output.message().decode()

        except StopIteration:
            valid = False

        if bridge_check != "0":
            valid = True

        if not valid:
            raise RuntimeError(
                'The switch does not appear to be trunking the correct VLANs for the {} bridge.'.
                format(bridge)
            )

    def run(self):
        """Add a new node to a Ganeti cluster."""
        print('Ready to add Ganeti node {} in the {} cluster'.format(self.fqdn, self.cluster))
        ask_confirmation('Is this correct?')

        if self.fqdn not in self.remote.query('A:ganeti-all').hosts:
            raise RuntimeError(
                '{} does have not have the Ganeti role applied. Please fix and re-run the cookbook'
                .format(self.fqdn)
            )

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

        self.is_valid_bridge('private')
        self.is_valid_bridge('public')

        if self.fqdn in self.remote.query('A:eqiad').hosts:
            self.is_valid_bridge('analytics')

        self.master.run_sync('gnt-node add --no-ssh-key-check -g "{group}" "{node}"'.format(
            group=self.group, node=self.fqdn))
        ask_confirmation('Has the node been added correctly?')

        self.master.run_sync('gnt-cluster verify')
        ask_confirmation('Verify that the cluster state looks correct.')

        self.master.run_sync('gnt-cluster verify-disks')
        ask_confirmation('Verify that the disk state looks correct.')
