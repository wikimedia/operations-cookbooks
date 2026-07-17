"""Manage BMC users via Redfish"""

import logging

from cumin import NodeSet
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.apiclient import APIClientResponseError
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.netbox import NetboxError
from spicerack.redfish import RedfishError

from getpass import getpass

logger = logging.getLogger(__name__)


class BMCUserMgmt(CookbookBase):
    """Manage BMC users via Redfish

    This cookbook ensures that BMC admin users are rolled out correctly:
    - wmfroot user with pwstore's management password
    - root user with pwstore's management password (Dell only)
    - ADMIN user with pwstore's management password (Supermicro only)

    If a new password is provided, it will be used instead of the management one.
    """

    owner_team = 'Infrastructure Foundations'

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return BMCUserMgmtRunner(args, self.spicerack)

    def argument_parser(self):
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument('query', help='Cumin query to match the host(s) to act upon.')
        parser.add_argument(
            '-t', '--task-id',
            help='An optional task ID to update, also used in log messages (i.e. T12345).'
        )
        parser.add_argument(
            '--new-password',
            action='store_true',
            help=('Set a new password for all the BMCs. '
                  'The new password will be asked during the execution.')
        )
        parser.add_argument(
            '--old-password',
            action='store_true',
            help=(
                'Force a prompt to pick the current management password. '
                'In the vast majority of use cases this will not be needed since '
                'the management password is picked up automatically by spicerack, '
                'but you may need it if you have changed the password on a test host '
                'and you want to roll it back without manual actions.'
            )
        )
        parser.add_argument(
            '--max-redfish-failures',
            default=20,
            help="The maximum Redfish failure to tolerate before bailing out."
        )

        return parser


class BMCUserMgmtRunner(CookbookRunnerBase):
    """Manage BMC users and passwords runner."""

    def __init__(self, args, spicerack):
        """Manage BMC users and passwords"""
        ensure_shell_is_durable()
        remote = spicerack.remote()
        self.spicerack = spicerack
        self.args = args
        self.remote_hosts = remote.query(args.query)
        self.max_redfish_failures = args.max_redfish_failures

        if not self.remote_hosts:
            raise RuntimeError('No hosts selected, bailing out.')

        ask_confirmation(
            f"The target hosts are {str(self.remote_hosts)}. Ok to proceed?")

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for {len(self.remote_hosts.hosts)} hosts'

    @property
    def lock_args(self):
        """Make the cookbook lock per-host."""
        return LockArgs(suffix=str(self.remote_hosts).split('.', 1)[0], concurrency=1, ttl=600)

    def _check_overall_failures(self):
        """Simple aux function to check if the overall failures are under the limit."""
        if len(self.host_status['fail_redfish']) > self.max_redfish_failures:
            raise RuntimeError("Too many redfish failures, bailing out!")

    def run(self):
        """Manage BMC users."""

        if self.args.old_password:
            mgmt_password = getpass(prompt='Current management password: ')
        else:
            mgmt_password = self.spicerack.management_password()

        new_password = ''
        if self.args.new_password:
            max_password_len = 20
            min_password_len = 16
            while True:
                new_password = getpass(
                    prompt=f"New Management Password (min {min_password_len} max {max_password_len} chars): "
                )
                if len(new_password) < min_password_len or len(new_password) > max_password_len:
                    logger.error(
                        'The new password must be between %s and %s chars long.',
                        min_password_len, max_password_len
                    )
                    continue
                repeat_password = getpass(prompt='Retype New Management Password: ')
                if repeat_password != new_password:
                    logger.error('The two passwords do not match!')
                    continue
                break

        self.host_status = {
            'success': NodeSet(),
            'fail_netbox': NodeSet(),
            'fail_redfish': NodeSet(),
            'fail_root_remove': NodeSet(),
        }
        password_to_enforce: str = new_password if new_password else mgmt_password
        for host in self.remote_hosts:
            try:
                hostname = str(host).split('.')[0]
                logger.info("\n===== Updating %s =====\n", hostname)
                netbox_server = self.spicerack.netbox_server(hostname)
                netbox_data = netbox_server.as_dict()
                if netbox_data['is_virtual']:
                    logger.info("The host %s is virtual, skipping.", hostname)
                    continue
                manufacturer_slug = netbox_data['device_type']['manufacturer']['slug']
            except NetboxError as error:
                logger.warning('Unable to get the mgmt address from Netbox for %s: %s', hostname, error)
                self.host_status['fail_netbox'].add(host)
                continue

            if  manufacturer_slug == 'dell':
                manufacturer_admin = 'root'
            elif manufacturer_slug == 'supermicro':
                manufacturer_admin = 'ADMIN'
            else:
                raise RuntimeError(f'Manufacturer not supported for host {hostname}')

            try:
                logger.info("Setting up a basic Redfish session with user %s", manufacturer_admin)
                # We need to use an admin user that we are sure it is present on every host
                # from a certain vendor.
                redfish = self.spicerack.redfish(hostname, username=manufacturer_admin, password=mgmt_password)
                # confirm redfish credentials work
                redfish.get_power_state()
            except RedfishError as e:
                logger.error("Failed to establish a Redfish session for %s: %s", hostname, e)
                self.host_status['fail_redfish'].add(host.hosts)
                self._check_overall_failures()

                # TODO: remove me after the first run on the whole server fleet
                # Some Supermicro's BMC don't have the management password set for the ADMIN
                # user, probably due to some past issues with provisioning.
                # Allow to test for the "root" user on Supermicros if the BMC replies with 401 (Not Authorized)
                # and remove this code bit once the cookbook has run on all the servers.
                if (manufacturer_slug == 'supermicro' and e.__cause__ is not None
                        and isinstance(e.__cause__, APIClientResponseError)
                        and e.__cause__.response is not None  # pylint: disable=no-member
                        and e.__cause__.response.status_code == 401):  # pylint: disable=no-member
                    try:
                        logger.info(
                            "The ADMIN user on Supermicro seems not configured with the right password, "
                            "trying to fallback to the root user."
                        )
                        redfish = self.spicerack.redfish(
                            hostname, username="root", password=mgmt_password)
                        # confirm redfish credentials work
                        redfish.get_power_state()
                    except (RedfishError, APIClientResponseError) as e:
                        logger.error("Failed to log in as root on a Supermicro: %s", e)
                        continue
                else:
                    continue


            # Global administrator user, used on Supermicro and Dells
            try:
                redfish.find_account("wmfroot")
            except RedfishError as e:
                logger.info(
                    "The wmfroot user on the BMC has not been created yet. "
                    "More info: %s", e)
                logger.info(
                    'Creating the wmfroot user on the BMC.')
                redfish.add_account('wmfroot', password_to_enforce)
            else:
                try:
                    logger.info(
                        "Updating the wmfroot user's password on the BMC.")
                    redfish.change_user_password('wmfroot', password_to_enforce)
                except (RedfishError, APIClientResponseError) as e:
                    logger.error(
                        "An error happened while trying to change the wmfroot's password "
                        "on %s: %s", hostname, e
                    )
                    self.host_status['fail_redfish'].add(host.hosts)
                    self._check_overall_failures()

            logger.info(
                "Updating the %s user's password on the BMC.", manufacturer_admin
            )
            try:
                redfish.change_user_password(manufacturer_admin, password_to_enforce)
            except (RedfishError, APIClientResponseError) as e:
                logger.error(
                    "An error happened while trying to change the %s's password "
                    "on %s: %s", manufacturer_admin, hostname, e
                )
                self.host_status['fail_redfish'].add(host.hosts)
                self._check_overall_failures()

            # TODO: add the delete account functionality to Spicerack to avoid any
            # manual actions for the user.
            if manufacturer_slug == 'supermicro':
                try:
                    redfish.find_account("root")
                    logger.info(
                        "Found root account on Supermicro, removing it: %s", hostname)
                except (RedfishError, APIClientResponseError) as e:
                    logger.error(
                        "An error happened while trying to find the root's account "
                        "on %s: %s", hostname, e
                    )
                    self.host_status['fail_redfish'].add(host.hosts)
                    self._check_overall_failures()
                    continue
                else:
                    try:
                        # For some reason, sometimes deleting an user on Supermicro doesn't
                        # work because some active sessions are still registered, even if
                        # there are none. From some tests it seems that simply calling
                        # the Sessions endpoint causes some sort of state reset, that allows
                        # the deletion.
                        redfish.request("GET", "/redfish/v1/SessionService/Sessions")
                        redfish.delete_account("root")
                    except (RedfishError, APIClientResponseError) as e:
                        logger.error(
                            "An error happened while trying to delete the root's account "
                            "on %s: %s", hostname, e
                        )
                        self.host_status['fail_root_remove'].add(host.hosts)
                        self._check_overall_failures()
                        continue

            logger.info("Checking that wmfroot and the manufacturer's admin can reach Redfish")
            for admin_user in ['wmfroot', manufacturer_admin]:
                redfish_newpass_check = self.spicerack.redfish(
                    hostname, username=admin_user, password=password_to_enforce
                )
                # Check if the new username and password work with a basic Redfish call
                try:
                    redfish_newpass_check.get_power_state()
                except RedfishError as e:
                    logger.error(
                        "Failed to verify get_power_state for user %s on host %s: %s",
                        admin_user, hostname, e
                    )
                    self.host_status['fail_redfish'].add(host.hosts)
                    break
            else:
                logger.info('password updated successfully for: %s', host.hosts)
                self.host_status['success'].add(host.hosts)

        message = '''
        The following hosts completed successfully:
            {}

        The following hosts were unable to get the management address from Netbox:
            {}

        The following hosts had redfish failures:
            {}

        The following Supermico hosts had issues when removing the root account:
            {}
            '''.format(
                    self.host_status['success'], self.host_status['fail_netbox'],
                    self.host_status['fail_redfish'], self.host_status['fail_root_remove']
                )
        logger.info(message)
