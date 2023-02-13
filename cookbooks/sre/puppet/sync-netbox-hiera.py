"""Update and deploy the hiera data generated from Netbox data."""
import json

from argparse import Namespace
from collections import defaultdict
from ipaddress import ip_network
from logging import getLogger
from pathlib import Path
from typing import DefaultDict, Optional, Union

import yaml

from requests.exceptions import RequestException

from wmflib.config import load_yaml_config
from wmflib.interactive import confirm_on_failure
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.reposync import RepoSyncNoChangeError


NETWORK_ROLES = ("cloudsw", "scs", "asw", "cr", "mr", "msw", "pfw", "pdu")

NETWORK_DEVICE_LIST_GQL = """
query ($role: [String], $status: [String]) {
    device_list(role: $role, status: $status) {
        name
        virtual_chassis {
            name
            master { name }
        }
        device_role { slug }
        device_type { slug }
        site { slug }
        tenant { name }
        primary_ip4 {
            dns_name
            address
        }
        primary_ip6 {
            dns_name
            address
        }
        interfaces {
            ip_addresses { address }
        }
    }
}
"""
DEVICE_LIST_GQL = """
query ($role: [String], $status: [String]) {
    device_list(role: $role, status: $status) {
        name
        status
        site { slug }
        tenant { name }
        rack {
            name
            location {
                slug
            }
        }
    }
}
"""
VM_LIST_GQL = """
query ($status: [String]) {
    virtual_machine_list(status: $status) {
        name
        status
        tenant { name }
        cluster {
            name
            group { name}
            site { slug }
        }
    }
}
"""
MGMT_LIST_GQL = """
query {
    interface_list(
        mgmt_only: true,
    ) {
        name
        ip_addresses {
            dns_name
        }
        device {
            status
            site { slug }
            tenant { name }
            rack {
                name
                location { slug }
            }
        }
    }
}
"""
PREFIX_LIST = """
query ($status: [String]) {
  prefix_list(status: $status) {
    site { slug }
    tenant { slug }
    role { slug }
    vlan { name }
    prefix
    status
    description
  }
}
"""


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
        parser = super().argument_parser()
        parser.add_argument(
            "-c",
            "--check",
            help="Check if there are new changes, forces a returncode of 1 if there are",
            action="store_true",
        )
        parser.add_argument(
            "-t", "--task-id", help="The Phabricator task ID (e.g. T12345)."
        )
        parser.add_argument(
            "--sha",
            help="If present the cookbook attempts to force a specific sha to the reposync clients.",
        )
        parser.add_argument("message", help="Commit message")

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return NetboxHieraRunner(args, self.spicerack)


class NetboxHieraRunner(CookbookRunnerBase):
    """Collect netbox hiera data."""

    # TODO: get rid of this hard coded directory
    client_repo_dir = "/srv/netbox-hiera"
    hiera_prefix = "profile::netbox"
    host_prefix = f"{hiera_prefix}::host"

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Init function.

        Arguments:
            args (Namespace): the parse arguments
            spicerack (Spicerack): An initiated spicerack object

        """
        if args.check and not spicerack.dry_run:
            # Force dry-run mode
            raise RuntimeError("check mode must also be run in --dry-run mode!")

        config = load_yaml_config(spicerack.config_dir / "netbox" / "config.yaml")

        self.logger = getLogger(__name__)
        self.args = args
        self.reposync = spicerack.reposync("netbox-hiera")
        self.puppetmasters = spicerack.remote().query("A:puppetmaster")
        self.reason = spicerack.admin_reason(args.message, task_id=args.task_id)
        self._api_url = f"{config['api_url']}graphql/"
        self._session = spicerack.requests_session(__name__, timeout=60)
        self._session.headers.update(
            {"Authorization": f"Token {config['api_token_ro']}"}
        )

    @property
    def runtime_description(self) -> str:
        """Required by API"""
        return f"generate netbox hiera data: {self.reason.quoted()}"

    def _gql_execute(self, query: str, variables: Optional[dict] = None) -> dict:
        """Parse the query into a gql query, execute and return the results

        Arguments:
            query: a string representing the gql query
            variables: A list of variables to send

        Results:
            dict: the results

        """
        data: dict[str, Union[str, dict]] = {"query": query}
        if variables is not None:
            data["variables"] = variables
        try:
            response = self._session.post(self._api_url, json=data, timeout=5 * 60)
            response.raise_for_status()
            return response.json()['data']
        except RequestException as error:
            raise RuntimeError(f"failed to fetch netbox data: {error}\n") from error
        except KeyError as error:
            raise RuntimeError(f"No data found in GraphQL response: {error}") from error

    def _network_devices(self, status: list[str], roles: tuple[str, ...]) -> dict:
        """Return the devices data.

        Arguments:
            roles: the netbox devices roles to filer on
            status: the netbox status to filter on

        """
        results = {}
        variables = {"role": roles, "status": status}
        devices = self._gql_execute(NETWORK_DEVICE_LIST_GQL, variables)['device_list']
        for device in devices:
            if device.get('primary_ip4') is None:
                self.logger.debug("%s has no primary ipv4 address", device['name'])
                continue
            # Only process the virtual chassis master, which has the ip address
            if (
                device.get('virtual_chassis') is not None
                and device['virtual_chassis']['master']['name'] != device['name']
            ):
                continue
            device_name = (
                device['virtual_chassis']['name'].split('.')[0]
                if device['virtual_chassis']
                else device['name']
            )
            data = {
                'primary_fqdn': device['primary_ip4']['dns_name'],
                'site': device['site']['slug'],
                'role': device['device_role']['slug'],
                'ipv4': device['primary_ip4']['address'].split('/')[0],
            }
            if (
                device['device_type']['slug'] == 'mx480'
                and device['device_role']['slug'] == 'cr'
            ):
                data['alarms'] = True
            if device.get('primary_ip6') is not None:
                data['ipv6'] = device['primary_ip6']['address'].split('/')[0]

            results[device_name] = data

        return results

    def _devices(self, status: list[str], roles: list[str]) -> dict:
        """Return the devices data.

        Arguments:
            roles: the netbox devices roles to filer on
            status: the netbox status to filter on

        Returns:
            dict: the management host data

        """
        results = {}
        variables = {"role": roles, "status": status}
        hosts = self._gql_execute(DEVICE_LIST_GQL, variables)['device_list']
        for host in hosts:
            # TODO: i think we should be able to filter this stuff out via the
            # GraphQL query directly, but i cant work out how to say is null
            if host['tenant'] is not None:
                continue

            data = {
                'status': host['status'].lower(),
                'location': {'site': host['site']['slug']},
            }
            if host['rack'] is None:
                continue
            data['location']['rack'] = host['rack']['name']
            data['location']['row'] = host['rack']['location']['slug']
            results[host['name']] = data

        return results

    def _virtual_hosts(self, status: list[str]) -> dict:
        """Return the Virtual machine data.

        Arguments:
            status: the netbox status to filter on

        Returns:
            dict: the management host data

        """
        results = {}
        variables = {"status": status}
        hosts = self._gql_execute(VM_LIST_GQL, variables)['virtual_machine_list']
        for host in hosts:
            # TODO: i think we should be able to filter this stuff out via the
            # GraphQL query directly
            if host['status'] not in ['ACTIVE', 'FAILED']:
                continue
            if host['tenant'] is not None:
                continue
            data = {
                'status': host['status'].lower(),
                'location': {
                    'site': host['cluster']['site']['slug'],
                    'ganeti_group': host['cluster']['name'],
                    'ganeti_cluster': host['cluster']['group']['name'],
                },
            }
            results[host['name']] = data

        return results

    def _mgmt_hosts(self) -> dict:
        """Return the mgmt_host data

        Returns:
            dict: the management host data

        """
        results = {}
        hosts = self._gql_execute(MGMT_LIST_GQL)['interface_list']
        for host in hosts:
            # TODO: i think we should be able to filter this stuff out via the
            # GraphQL query directly
            if not host['ip_addresses'] or not host['ip_addresses'][0]['dns_name']:
                continue

            device = host['device']
            if device['tenant'] is not None:
                continue
            if device['status'] in ['OFFLINE', 'PLANNED', 'DECOMMISSIONING', 'FAILED']:
                continue

            data = {
                'row': device['rack']['location']['slug'],
                'rack': device['rack']['name'],
                'site': device['site']['slug'],
            }

            address = host['ip_addresses'][0]['dns_name']
            results[address] = data

        return results

    def _prefixes(self, status: list[str]):
        """Fetch and format the list of prefixes from netbox.

        Arguments:
            status: the netbox status to filter on

        """
        variables = {"status": status}
        prefix_list = self._gql_execute(PREFIX_LIST, variables)['prefix_list']
        prefixes: DefaultDict[str, dict] = defaultdict(dict)
        for prefix_data in prefix_list:
            prefix = prefixes[prefix_data['prefix']]

            prefix['public'] = ip_network(prefix_data['prefix']).is_global
            for key, value in prefix_data.items():
                # skip empty values
                if value is None or key == "prefix":
                    continue
                if key == 'status':
                    value = value.lower()
                # collapse the slug and name
                if isinstance(value, dict):
                    for collapse_key in ['slug', 'name']:
                        if collapse_key in value:
                            value = value.get(collapse_key, value)
                prefix[key] = value

        return prefixes

    def _write_hiera_files(self, out_dir: Path) -> None:
        """Write out all the hiera files.

        Arguments:
            out_dir (Path): The directory to write the data

        """
        valid_status = ['active', 'failed']
        hosts = self._virtual_hosts(valid_status) | self._devices(
            valid_status, ['server']
        )
        hosts_dir = out_dir / "hosts"
        hosts_dir.mkdir()
        for host, host_data in hosts.items():
            host_path = hosts_dir / f"{host}.yaml"
            hiera_data = {f"{self.host_prefix}::{k}": v for k, v in host_data.items()}
            with host_path.open("w") as host_fh:
                yaml.safe_dump(hiera_data, host_fh, default_flow_style=False)

        common_path = out_dir / "common.yaml"
        mgmt_hosts = self._mgmt_hosts()
        prefixes = self._prefixes(['active'])
        network_devices = self._network_devices(['active'], NETWORK_ROLES)
        # use json to get rid of defaultdicts
        common_data = json.loads(json.dumps({
            f"{self.hiera_prefix}::data::mgmt": mgmt_hosts,
            f"{self.hiera_prefix}::data::prefixes": prefixes,
            f"{self.hiera_prefix}::data::network_devices": network_devices,
        }))
        with common_path.open("w") as common_fh:
            yaml.safe_dump(common_data, common_fh, default_flow_style=False)

    def update_puppetmasters(self, hexsha: str) -> None:
        """Update the puppet masters to a specific hash

        Arguments:
            hexsha (str): The hexsha to checkout

        """
        commands = [
            f"git -C {self.client_repo_dir} fetch",
            f"git -C {self.client_repo_dir} merge --ff-only {hexsha}",
        ]
        confirm_on_failure(self.puppetmasters.run_sync, *commands)

    def run(self) -> int:
        """Generate data"""
        if self.args.sha:
            self.reposync.force_sync()
            self.update_puppetmasters(self.args.sha)
            return 0
        try:
            with self.reposync.update(str(self.reason)) as working_dir:
                self._write_hiera_files(working_dir)
        except RepoSyncNoChangeError:
            print("No Changes to apply")
            return 0
        if self.reposync.hexsha is None:
            raise RuntimeError(
                "No hexsha value received from reposync.  Something went wrong!"
            )
        if self.args.check:
            return 1
        self.update_puppetmasters(self.reposync.hexsha)
        return 0
