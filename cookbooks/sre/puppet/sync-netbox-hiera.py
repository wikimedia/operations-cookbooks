"""Update and deploy the hiera data generated from Netbox data."""
import asyncio
import json
import inspect

from argparse import Namespace
from collections import defaultdict
from dataclasses import dataclass
from ipaddress import ip_network
from logging import getLogger
from pathlib import Path
from time import time
from typing import DefaultDict, Optional, Union

import yaml

from aiohttp import ClientSession, ClientResponseError

from wmflib.config import load_yaml_config
from wmflib.interactive import confirm_on_failure
from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.reposync import RepoSyncNoChangeError


NETWORK_ROLES = ("cloudsw", "scs", "asw", "cr", "mr", "msw", "pfw", "pdu")

NETWORK_DEVICE_LIST_GQL = """
query ($role: [String!], $status: [String!]) {
    device_list(filters: {role: $role, status: $status}) {
        name
        virtual_chassis {
            name
            master { name }
        }
        role { slug }
        device_type {
            slug
            manufacturer { slug }
        }
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
    }
}
"""
DEVICE_LIST_GQL = """
query ($role: [String!], $status: [String!]) {
    device_list(filters: {role: $role, status: $status}) {
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
query ($status: [String!]) {
    virtual_machine_list(filters: {status: $status}) {
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
    interface_list(filters: {mgmt_only: true}) {
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
            role { slug }
        }
    }
}
"""
PREFIX_LIST = """
query ($status: [String!]) {
  prefix_list(filters: {status: $status}) {
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


@dataclass
class NetboxData:
    """Data class to hold netbox data"""

    prefixes: dict
    virtual_hosts: dict
    network_devices: dict
    baremetal_hosts: dict
    mgmt_hosts: dict

    @property
    def hosts(self):
        """Return both virtual and bare metal hosts."""
        return self.baremetal_hosts | self.virtual_hosts


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

    hiera_prefix = "profile::netbox"
    host_prefix = f"{hiera_prefix}::host"
    # Customize the cookbook's lock
    max_concurrency = 1
    lock_ttl = 300

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
        self.puppetservers = spicerack.remote().query("A:puppetserver")
        self.reason = spicerack.admin_reason(args.message, task_id=args.task_id)
        self._uri = f"{config['api_url']}graphql/"
        self._headers = {"Authorization": f"Token {config['api_token_ro']}"}

    @property
    def runtime_description(self) -> str:
        """Required by API"""
        return f"generate netbox hiera data: {self.reason.quoted()}"

    async def _gql_execute(self, query: str, variables: Optional[dict] = None) -> dict:
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
        calling_method = "Unknown"
        current_frame = inspect.currentframe()
        if current_frame is not None and current_frame.f_back is not None:
            calling_method = current_frame.f_back.f_code.co_name

        async with ClientSession(headers=self._headers, raise_for_status=True) as session:
            try:
                self.logger.debug("fetching: %s", calling_method)
                start = time()
                async with session.post(self._uri, json=data) as response:
                    data = await response.json()
                    self.logger.debug("received: %s (%fs)", calling_method, time() - start)
                    if not isinstance(data['data'], dict):
                        raise ValueError(f"received unexpected response: {data}")
                    return data['data']
            except ClientResponseError as error:
                raise RuntimeError(f"failed to fetch netbox data: {error}\n") from error
            except KeyError as error:
                raise RuntimeError(
                    f"No data found in GraphQL response: {error}"
                ) from error

    async def _network_devices(self, status: list[str], roles: tuple[str, ...]) -> dict:
        """Return the devices data.

        Arguments:
            roles: the netbox devices roles to filer on
            status: the netbox status to filter on

        """
        results = {}
        variables = {"role": roles, "status": status}
        devices = await self._gql_execute(NETWORK_DEVICE_LIST_GQL, variables)
        for device in devices['device_list']:
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
                'manufacturer': device['device_type']['manufacturer']['slug'],
                'site': device['site']['slug'],
                'role': device['role']['slug'],
                'ipv4': device['primary_ip4']['address'].split('/')[0],
            }

            if (
                device['device_type']['slug'] == 'mx480'
                and device['role']['slug'] == 'cr'
            ):
                data['alarms'] = True
            if device.get('primary_ip6') is not None:
                data['ipv6'] = device['primary_ip6']['address'].split('/')[0]

            results[device_name] = data
        return results

    async def _baremetal_hosts(self, status: list[str], roles: list[str]) -> dict:
        """Return the devices data.

        Arguments:
            roles: the netbox devices roles to filer on
            status: the netbox status to filter on
            results: the object to update

        Returns:
            dict: the management host data

        """
        results = {}
        variables = {"role": roles, "status": status}
        hosts = await self._gql_execute(DEVICE_LIST_GQL, variables)
        for host in hosts['device_list']:
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

    async def _virtual_hosts(self, status: list[str]) -> dict:
        """Return the Virtual machine data.

        Arguments:
            status: the netbox status to filter on

        Returns:
            dict: the management host data

        """
        results = {}
        variables = {"status": status}
        hosts = await self._gql_execute(VM_LIST_GQL, variables)
        for host in hosts['virtual_machine_list']:
            # TODO: i think we should be able to filter this stuff out via the
            # GraphQL query directly
            # TODO: check if the 'if' block below is useless as we filter the status in the query
            if host['status'] not in ['active', 'failed']:
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

    async def _mgmt_hosts(self) -> dict:
        """Return the mgmt_host data

        Returns:
            dict: the management host data

        """
        results = {}
        hosts = await self._gql_execute(MGMT_LIST_GQL)
        for host in hosts['interface_list']:
            # TODO: i think we should be able to filter this stuff out via the
            # GraphQL query directly
            if not host['ip_addresses'] or not host['ip_addresses'][0]['dns_name']:
                continue

            device = host['device']
            if device['tenant'] is not None:
                continue
            if device['status'] in ['offline', 'planned', 'decommissioning', 'failed']:
                continue

            data = {
                'row': device['rack']['location']['slug'],
                'rack': device['rack']['name'],
                'role': device['role']['slug'],
                'site': device['site']['slug'],
            }

            address = host['ip_addresses'][0]['dns_name']
            results[address] = data

        return results

    async def _prefixes(self, status: list[str]):
        """Fetch and format the list of prefixes from netbox.

        Arguments:
            status: the netbox status to filter on

        """
        variables = {"status": status}
        prefix_list = await self._gql_execute(PREFIX_LIST, variables)
        prefixes: DefaultDict[str, dict] = defaultdict(dict)
        for prefix_data in prefix_list['prefix_list']:
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

    async def _fetch_data(self) -> NetboxData:
        """Fetch the data from netbox"""
        valid_status = ['active', 'failed']
        baremetal_hosts_task = asyncio.create_task(self._baremetal_hosts(valid_status, ['server']))
        virtual_hosts_task = asyncio.create_task(self._virtual_hosts(valid_status))
        mgmt_hosts_task = asyncio.create_task(self._mgmt_hosts())
        prefixes_task = asyncio.create_task(self._prefixes(['active']))
        network_devices_task = asyncio.create_task(
            self._network_devices(['active'], NETWORK_ROLES)
        )

        baremetal_hosts = await baremetal_hosts_task
        virtual_hosts = await virtual_hosts_task
        mgmt_hosts = await mgmt_hosts_task
        prefixes = await prefixes_task
        network_devices = await network_devices_task
        return NetboxData(
            virtual_hosts=virtual_hosts,
            network_devices=network_devices,
            baremetal_hosts=baremetal_hosts,
            mgmt_hosts=mgmt_hosts,
            prefixes=prefixes
        )

    async def _write_hiera_files(self, out_dir: Path) -> None:
        """Write out all the hiera files.

        Arguments:
            out_dir (Path): The directory to write the data

        """
        common_path = out_dir / "common.yaml"
        hosts_dir = out_dir / "hosts"
        hosts_dir.mkdir()
        netbox_data = await self._fetch_data()

        for host, host_data in netbox_data.hosts.items():
            host_path = hosts_dir / f"{host}.yaml"
            hiera_data = {f"{self.host_prefix}::{k}": v for k, v in host_data.items()}
            with host_path.open("w") as host_fh:
                yaml.safe_dump(hiera_data, host_fh, default_flow_style=False)

        # use json to get rid of defaultdicts
        common_data = json.loads(
            json.dumps(
                {
                    f"{self.hiera_prefix}::data::mgmt": netbox_data.mgmt_hosts,
                    f"{self.hiera_prefix}::data::prefixes": netbox_data.prefixes,
                    f"{self.hiera_prefix}::data::network_devices": netbox_data.network_devices,
                }
            )
        )
        with common_path.open("w") as common_fh:
            yaml.safe_dump(common_data, common_fh, default_flow_style=False)

    def update_puppetservers(self, hexsha: str) -> None:
        """Update the puppet masters to a specific hash

        Arguments:
            hexsha (str): The hexsha to checkout

        """
        client_repo_dir = "/srv/git/netbox-hiera"
        commands = [
            f"sudo -u gitpuppet git -C {client_repo_dir} fetch",
            f"sudo -u gitpuppet git -C {client_repo_dir} merge --ff-only {hexsha}",
        ]
        confirm_on_failure(self.puppetservers.run_sync, *commands)

    def update_puppetmasters(self, hexsha: str) -> None:
        """Update the puppet masters to a specific hash

        Arguments:
            hexsha (str): The hexsha to checkout

        """
        client_repo_dir = "/srv/netbox-hiera"
        commands = [
            f"git -C {client_repo_dir} fetch",
            f"git -C {client_repo_dir} merge --ff-only {hexsha}",
        ]
        confirm_on_failure(self.puppetmasters.run_sync, *commands)

    def run(self) -> int:
        """Generate data"""
        if self.args.sha:
            self.reposync.force_sync()
            self.update_puppetmasters(self.args.sha)
            self.update_puppetservers(self.args.sha)
            return 0
        try:
            with self.reposync.update(str(self.reason)) as working_dir:
                asyncio.run(self._write_hiera_files(working_dir))
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
        self.update_puppetservers(self.reposync.hexsha)
        return 0
