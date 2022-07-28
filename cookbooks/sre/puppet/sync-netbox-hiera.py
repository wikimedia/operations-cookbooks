"""Update and deploy the hiera data generated from Netbox data."""

from argparse import ArgumentParser, Namespace
from os import PathLike
from typing import Dict

import yaml

from requests.exceptions import RequestException

from wmflib.config import load_yaml_config
from wmflib.interactive import confirm_on_failure
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase
from spicerack.reposync import RepoSyncNoChangeError


class NetboxHiera(CookbookBase):
    """Update and deploy the hiera data generated from Netbox data.

    Run the script that generates hiera data on the Netbox host to update the
    exposed git repository with the data and then deploy them to the
    puppetmaster hosts, reloading apache.

    Usage example:
        cookbook sre.puppet.sync-netbox-hiera -t T12345 'Decommissioned mw12[22-35]'

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = ArgumentParser(
            description=self.__doc__,
            formatter_class=ArgparseFormatter,
        )
        parser.add_argument(
            '-c', '--check', help='Check if there are new changes, forces a returncode of 1 if there are'
        )
        parser.add_argument(
            '-t', '--task-id', help='The Phabricator task ID (e.g. T12345).'
        )
        parser.add_argument(
            '--sha', help='If present the cookbook attempts to force a specific sha to the reposync clients.'
        )
        parser.add_argument('message', help='Commit message')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return NetboxHieraRunner(args, self.spicerack)


class NetboxHieraRunner(CookbookRunnerBase):
    """Collect netbox hiera data."""

    # TODO: get rid of this hard coded directory
    client_repo_dir = '/srv/netbox-hiera'
    hiera_prefix = 'profile::netbox'
    host_prefix = f'{hiera_prefix}::host'

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Init function.

        Arguments:
            args (Namespace): the parse arguments
            spicerack (Spicerack): An initiated spicerack object

        """
        config = load_yaml_config(spicerack.config_dir / "netbox" / "config.yaml")
        if args.check and not spicerack.dry_run:
            # Force dry-run mode
            raise RuntimeError("check mode must also be run in --dry-run mode!")
        self.args = args
        self.reposync = spicerack.reposync('netbox-hiera')
        self.puppetmasters = spicerack.remote().query("A:puppetmaster")
        self.reason = spicerack.admin_reason(args.message, task_id=args.task_id)
        self.api_url = f"{config['api_url'].rstrip('/')}:8443/hiera_export.HieraExport"
        self.session = spicerack.requests_session(__name__, timeout=30)
        self.session.headers.update(
            {"Authorization": f"Token {config['api_token_ro']}"}
        )

    @property
    def runtime_description(self) -> str:
        """Required by API"""
        return f"generate netbox hiera data: {self.reason.quoted()}"

    def _get_netbox_data(self) -> Dict:
        """Fetch netbox data.

        Returns:
            dict: dictionary of netbox objects

        """
        try:
            response = self.session.get(self.api_url, json={})
            response.raise_for_status()
        except RequestException as error:
            raise RuntimeError(f"failed to fetch netbox data: {error}") from error

        return response.json()

    def _write_hiera_files(self, out_dir: PathLike) -> None:
        """Write out all the hiera files.

        Arguments:
            out_dir (PathLike): The directory to write the data

        """
        data = self._get_netbox_data()
        hosts_dir = out_dir / 'hosts'
        hosts_dir.mkdir()
        for host, data in data['hosts'].items():
            host_path = hosts_dir / f"{host}.yaml"
            data = {f'{self.host_prefix}::{k}': v for k, v in data.items()}
            with host_path.open('w') as host_fh:
                yaml.safe_dump(data, host_fh, default_flow_style=False)

    def update_puppetmasters(self, hexsha: str) -> None:
        """Update the puppet masters to a specific hash

        Arguments:
            hexsha (str): The hexsha to checkout

        """
        commands = [f'git -C {self.client_repo_dir} fetch',
                    f'git -C {self.client_repo_dir} merge --ff-only {hexsha}']
        confirm_on_failure(self.puppetmasters.run_sync, *commands)

    def run(self) -> None:
        """Generate data"""
        if self.args.sha:
            self.reposync.force_sync()
            self.update_puppetmasters(self.args.sha)
            return 0
        try:
            with self.reposync.update(str(self.reason)) as working_dir:
                self._write_hiera_files(working_dir)
        except RepoSyncNoChangeError:
            print('No Changes to apply')
            return 0
        if self.reposync.hexsha is None:
            raise RuntimeError("No hexsha value received from reposync.  Something went wrong!")
        if self.args.check:
            return 1
        self.update_puppetmasters(self.reposync.hexsha)
        return 0
