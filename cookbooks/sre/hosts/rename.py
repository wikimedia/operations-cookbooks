"""Rename a physical host through a re-image."""
import logging

from pprint import pformat

from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from cookbooks.sre import PHABRICATOR_BOT_CONFIG_FILE

logger = logging.getLogger(__name__)


class Rename(CookbookBase):
    """Rename a physical host through a re-image.

    Actions performed:
        * Rename the host and matching DNS names in Netbox
        * Run the DNS cookbook to propagate the change
        * Update the switch port description
        * Remove from DebMonitor and Puppet
        * Supports rollback on errors

    Possible improvments:
        * Check that the new name is in the same repos than the old one

    Usage:
        cookbook sre.hosts.rename -t T000000 foo1001 bar1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('-t', '--task-id', help='the Phabricator task ID to update and refer (i.e.: T12345)')
        parser.add_argument('old_name', help='Short hostname of the host to rename, not FQDN')
        parser.add_argument('new_name', help='Future name, still not FQDN')

        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return RenameRunner(args, self.spicerack)


class RenameRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'from {self.old_name} to {self.new_name}'

    def __init__(self, args, spicerack):
        """Initiliaze the rename runner."""
        ensure_shell_is_durable()

        self.old_name = args.old_name
        self.new_name = args.new_name
        self.task_id = args.task_id
        self.netbox_server = spicerack.netbox_server(self.old_name, read_write=True)
        self.old_fqdn = self.netbox_server.fqdn
        self.puppet_master = spicerack.puppet_master()
        self.puppet_server = spicerack.puppet_server()
        self.debmonitor = spicerack.debmonitor()
        self.run_cookbook = spicerack.run_cookbook
        self.alerting_host = spicerack.alerting_hosts([self.old_name])
        self.redfish = spicerack.redfish(self.old_name)
        self.redfish.check_connection()
        self.spicerack = spicerack

        self.actions = spicerack.actions
        self.host_actions = self.actions[self.old_name]
        self.reason = spicerack.admin_reason('Host renaming', task_id=self.task_id)

        # States for rollback purposes
        self.dns_propagated = False
        self.netbox_name_changed = False
        self.switch_description_changed = False
        self.idrac_name_updated = False

        if self.task_id is not None:
            self.phabricator = spicerack.phabricator(PHABRICATOR_BOT_CONFIG_FILE)
        else:
            self.phabricator = None
        ask_confirmation(f"Is {self.new_name} in Puppet's site.pp and preseed.yaml?")

    def run_raise(self, name: str, args: list):
        """Run a cookbook and raise an error if return code is non-zero."""
        ret = self.run_cookbook(name, args)
        if ret:
            raise RuntimeError(f'Failed to run cookbook {name}')

    def run(self):
        """Run the cookbook."""
        self.alerting_host.downtime(self.reason)
        self.host_actions.success('✔️ Downtimed host on Icinga/Alertmanager')
        self.netbox_server.name = self.new_name
        self.host_actions.success('✔️ Netbox updated')
        self.netbox_name_changed = True

        response = self.redfish.request('patch',
                                        f'{self.redfish.oob_manager}/EthernetInterfaces/NIC.1',
                                        json={'HostName': self.new_name}).json()
        logger.debug('Redfish response:\n%s', pformat(response))

        self.idrac_name_updated = True
        self.host_actions.success('✔️ IDRAC updated')

        self.propagate_dns()
        confirm_on_failure(self.run_raise, 'sre.network.configure-switch-interfaces', [self.new_name])
        self.host_actions.success('✔️ Switch description updated')
        self.switch_description_changed = True

        self.debmonitor.host_delete(self.old_fqdn)
        self.host_actions.success('✔️ Removed from DebMonitor')
        self.puppet_master.delete(self.old_fqdn)
        self.puppet_server.delete(self.old_fqdn)
        self.host_actions.success('✔️ Removed from Puppet master and PuppetDB')
        # Too late for a rollback, setting it back to False
        self.netbox_name_changed = False
        self.host_actions.success('Rename completed 👍 - now please run the re-image cookbook '
                                  'on the new name with --new')
        # Comment on the Phabricator task
        self._phab_dump()

        if self.host_actions.has_failures:
            return 1
        return 0

    def _phab_dump(self):
        if self.phabricator is not None:
            action = 'executed with errors' if self.host_actions.has_failures else 'completed'
            self.phabricator.task_comment(
                self.task_id,
                (f'Cookbook {__name__} started by {self.reason.owner} {self.runtime_description} {action}:'
                    f'\n{self.actions}\n'),
            )

    def propagate_dns(self, rollback: bool = False):
        """Run the sre.dns.netbox cookbook to propagate the DNS records."""
        if rollback:
            message = f'Rolling back {self.new_name} to {self.old_name}'
        else:
            message = f'Renaming {self.old_name} to {self.new_name}'
        confirm_on_failure(self.run_raise, 'sre.dns.netbox', [message])
        self.host_actions.success('✔️ DNS updated')
        self.dns_propagated = True
        # TODO do we care about wiping DNS cache?

    def rollback(self):
        """Rollback the various changes depending on the process advancements on failure."""
        if not self.netbox_name_changed:
            self.host_actions.warning('⚠️//Rollback initiated but nothing to rollback (too soon or too late).//⚠️')
            self._phab_dump()
        # Workaround bug https://github.com/netbox-community/pynetbox/issues/586
        # by refreshing netbox_server, otherwise the IPs/vlan are not rolled back despite Spicerack saying so
        self.netbox_server = self.spicerack.netbox_server(self.new_name, read_write=True)
        self.netbox_server.name = self.old_name
        self.host_actions.success('✔️ Netbox rolled back')
        if self.switch_description_changed:
            confirm_on_failure(self.run_raise, 'sre.network.configure-switch-interfaces', [self.old_name])
        if self.dns_propagated:
            self.propagate_dns(rollback=True)
        if self.idrac_name_updated:
            response = self.redfish.request('patch',
                                            f'{self.redfish.oob_manager}/EthernetInterfaces/NIC.1',
                                            json={'HostName': self.old_name}).json()
            logger.debug('Redfish response:\n%s', pformat(response))
            self.host_actions.success('✔️ iDRAC rolled back')

        self.host_actions.warning('⚠️//Renaming failed but rollback succedded//⚠️ '
                                  'Please check the logs for the reason and follow up with I/F if needed.')
        self._phab_dump()
