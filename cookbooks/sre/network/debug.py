"""Configure the switch interfaces of a given host"""
import argparse
import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE
from cookbooks.sre.network import get_junos_interface, get_junos_logs, get_junos_optics

logger = logging.getLogger(__name__)


class Debug(CookbookBase):
    """Gather troubleshooting informations from network devices

    Usage example:
        cookbook -d sre.network.debug circuit 123
        cookbook -d sre.network.debug interface 345
        cookbook sre.network.debug -t T123456 interface 678
    """

    argument_task_required = False

    def validate_object_id(self, value: str):
        """Ensure the user provides a correctly formated object_id"""
        try:
            int(value)
        except ValueError as exc:
            if value.count(':') == 0:
                raise argparse.ArgumentTypeError('object_id must be int or colon seperated string') from exc
        return value

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('entity', choices=['circuit', 'interface'])
        parser.add_argument('object_id',
                            type=self.validate_object_id,
                            help='Netbox numerical ID or device:interface couple (eg. cr1-ulsfo:xe-1/2/3')
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return DebugRunner(args, self.spicerack)


class DebugRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the debug runner."""
        self.netbox = spicerack.netbox()
        self.remote = spicerack.remote()
        self.args = args
        self.task_comment = []
        self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)

    def run(self):
        """Required by Spicerack API."""
        if self.args.entity == 'interface':
            if ':' in self.args.object_id:
                device_name, interface_name = self.args.object_id.split(':', 1)
                nb_interface = self.netbox.api.dcim.interfaces.get(device=device_name,
                                                                   name=interface_name)
                if not nb_interface:
                    raise RuntimeError(f"No Netbox interface matching {self.args.object_id}")
                netbox_id = nb_interface.id
            else:
                netbox_id = self.args.object_id
            # "z" references the remote side of the interface
            z_nb_interface = self.debug_interface(netbox_id)
            if z_nb_interface:
                self.debug_interface(z_nb_interface.id)
        elif self.args.entity == 'circuit':
            self.debug_circuit(self.args.object_id)

        if self.task_comment:
            self.phabricator.task_comment(
                self.args.task_id,
                f"===== Automated diagnostic {self.runtime_description}" + '\n'.join(self.task_comment))

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"for Netbox {self.args.entity} ID {self.args.object_id}"

    def debug_interface(self, netbox_id):
        """Debug commands and light analysis for single interface."""
        connected_int = None
        nb_interface = self.netbox.api.dcim.interfaces.get(netbox_id)
        if not nb_interface:
            raise RuntimeError(f"No Netbox interface with ID {netbox_id}")
        if nb_interface.connected_endpoints_type == 'dcim.interface':
            connected_int = nb_interface.connected_endpoints[0]
        message = f'Interface {nb_interface.device}:{nb_interface.name}'
        logger.info('%s', message)
        if nb_interface.device.device_type.manufacturer.slug != 'juniper':
            logger.info('Not a Juniper device, skipping')
            return connected_int
        self.task_comment.append(f'\n---\n**{message}**')
        if nb_interface.connected_endpoints and not nb_interface.connected_endpoints_reachable:
            message = '⚠️ Endpoint unreachable according to Netbox, check the path and cables status'
            logger.error('%s', message)
            self.task_comment.append(f'{message}')
        device_fqdn = nb_interface.device.primary_ip.dns_name
        remote_host = self.remote.query('D{' + device_fqdn + '}')
        int_status = get_junos_interface(remote_host, nb_interface.name)
        if int_status:
            for k, v in int_status.items():
                prefix = '⚠️ ' if v == 'down' or (k == 'errors' and v) else ''
                message = f'{prefix} {k}: {v}'
                logger.info('%s', message)
                self.task_comment.append(f'- {message}')

        optics_levels = get_junos_optics(remote_host, nb_interface.name)
        if optics_levels:
            for k, v in optics_levels.items():
                prefix = '⚠️ ' if 'dbm' in k and float(v) < -20 else ''
                message = f'{prefix} {k}: {v}'
                logger.info('%s', message)
                self.task_comment.append(f'- {message}')
        logs = get_junos_logs(remote_host, nb_interface.name)
        if logs:
            logger.info('%s', logs)
            self.task_comment.append(f'```name=Logs for {remote_host}:{nb_interface.name}\n' + logs + '\n```')
        return connected_int

    def debug_circuit(self, netbox_id):
        """Debug commands and light analysis for circuits."""
        nb_circuit = self.netbox.api.circuits.circuits.get(netbox_id)
        if not nb_circuit:
            raise RuntimeError(f"No Netbox circuit with ID {netbox_id}")
        logger.info('%s circuit %s', nb_circuit.provider, nb_circuit.cid)
        for termination_side in ('termination_a', 'termination_z'):
            try:
                termination = getattr(nb_circuit, termination_side)
                if termination.link_peers_type == 'dcim.interface':
                    self.debug_interface(termination.link_peers[0].id)
            except AttributeError:
                pass
