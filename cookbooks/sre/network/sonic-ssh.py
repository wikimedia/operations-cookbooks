"""Manage SONiC users SSH keys"""

from pathlib import Path
from typing import Optional

import logging

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.remote import RemoteExecutionError
from wmflib.config import load_yaml_config

from cookbooks.sre.network import parse_results

logger = logging.getLogger(__name__)


class SonicSsh(CookbookBase):
    """Workaround the lack of SSH key support in SONiC's API

    Depending on the arguments passed it will connect to the device
    and create/update the user's ~/.ssh/authorized_keys file based on the homer-public repo.

    Usage examples:
        cookbook sre.network.sonic-ssh lsw1-e8-eqiad
        cookbook sre.network.sonic-ssh all (TODO)
        cookbook sre.network.sonic-ssh all username (TODO)
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        # TODO maybe add a <user> parameter to tackle single users
        parser = super().argument_parser()
        parser.add_argument('device', help='Short hostname.')
        parser.add_argument('--homer-public-path', default='/srv/homer/public/', type=Path,
                            help="Path to the local homer-public directory.")
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return SonicSshRunner(args, self.spicerack)


class SonicSshRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initiliaze the SONiC-SSH runner."""
        self.netbox = spicerack.netbox()
        self.verbose = spicerack.verbose
        self.dry_run = spicerack.dry_run
        self.remote = spicerack.remote()
        self.device = args.device
        self.homer_public_path = args.homer_public_path
        self.http_session = spicerack.requests_session(__name__)

        self.netbox_device = self.netbox.api.dcim.devices.get(name=self.device)
        if not self.netbox_device:
            raise RuntimeError(f'{self.device}: device not found in Netbox')
        if self.netbox_device.role.slug != 'asw' or self.netbox_device.device_type.manufacturer.slug != "dell":
            raise RuntimeError(f'{self.device}: invalid role or manufacturer (MUST be asw and dell)')
        try:
            self.device_fqdn = self.netbox_device.primary_ip.dns_name
        except AttributeError as exc:
            raise RuntimeError(f'{self.device}: Missing primary IP in Netbox.') from exc
        if not self.device_fqdn:
            raise RuntimeError(f'{self.device}: Missing DNS name (FQDN) on primary IP in Netbox.')
        self.remote_host = self.remote.query('D{' + self.device_fqdn + '}')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"for network device {self.device}"

    def get_wanted_users(self) -> dict:
        """Get the list of users from the source of truth."""
        wanted_users = {}
        # From homer-public until T335870 is done
        common_config = load_yaml_config(self.homer_public_path / 'config' / 'common.yaml')
        for user in common_config['users']:
            wanted_users[user['name']] = user['sshkeys']
        return wanted_users

    def get_authorized_keys(self, username: str) -> Optional[list]:
        """Return the currently configured ssh keys or None."""
        try:
            results_raw = self.remote_host.run_sync(f"sudo cat /home/{username}/.ssh/authorized_keys",
                                                    print_output=self.verbose,
                                                    print_progress_bars=False,
                                                    is_safe=True)
            parsed_result = parse_results(results_raw)
            return parsed_result.split('\n') if parsed_result else None
        except RemoteExecutionError:  # TODO: doesn't work with is_safe?
            return None

    def get_configured_users(self) -> dict:
        """Get the list of configured users from the device."""
        # By parsing /etc/passwd

        configured_users = {}

        # Regular users all have "sonic-launch-shell" as bash
        results_raw = self.remote_host.run_sync("cat /etc/passwd | grep sonic-launch-shell",
                                                print_output=self.verbose,
                                                print_progress_bars=False,
                                                is_safe=True)
        parsed_result = parse_results(results_raw)
        if parsed_result:
            for line in parsed_result.split('\n'):
                user_config = line.split(":")
                username = user_config[0]
                # TODO leverage cumin multi commands thingy?
                configured_users[username] = self.get_authorized_keys(username)
        return configured_users

    def set_authorized_keys(self, username: str, pubkeys: list):
        """Configure ssh keys for a user."""
        pubkeys_string = "\n".join(pubkeys)
        commands = [f'sudo mkdir -p /home/{username}/.ssh',
                    f'sudo chown {username}:{username} /home/{username}/.ssh',
                    f'echo "{pubkeys_string}" | sudo tee /home/{username}/.ssh/authorized_keys',
                    f'sudo chown {username} /home/{username}/.ssh/authorized_keys']
        self.remote_host.run_sync(*commands, print_output=self.verbose, print_progress_bars=False)

    def run(self):
        """Required by Spicerack API."""
        configured_users = self.get_configured_users()
        wanted_users = self.get_wanted_users()
        configured_names = set(configured_users.keys())
        wanted_names = set(wanted_users.keys())
        if configured_names != wanted_names:
            logger.warning("Discrepancy between wanted and configured users, please run Homer to fix.")
            logger.warning("Should be removed: %s.", configured_names.difference(wanted_names))
            logger.warning("Should be created: %s.", wanted_names.difference(configured_names))

        for username in configured_names & wanted_names:
            if wanted_users[username] != configured_users[username]:
                logger.info("Setting SSH key(s) for user %s.", username)
                self.set_authorized_keys(username, wanted_users[username])
