"""Pool/depool all servers from a given rack."""
import json
import logging

from collections import defaultdict
from urllib.request import urlopen

from spicerack.cookbook import CookbookBase, CookbookInitSuccess, CookbookRunnerBase
from spicerack.netbox import NetboxServer
from spicerack.remote import RemoteExecutionError
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import ask_confirmation
# TODO wishlist: add Phab logging
logger = logging.getLogger(__name__)


class DepoolRack(CookbookBase):
    """Depool (or repool) all services from a given rack.

    Usage example:
        cookbook sre.network.depool-rack --site codfw --rack C5 depool --show

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument("action", choices=("pool", "depool"))
        parser.add_argument("--site", choices=ALL_DATACENTERS)
        parser.add_argument("--rack", type=str.upper, help="Rack name (A1, D8...)")
        parser.add_argument("--show", action="store_true",
                            help="Stop after listing all the actions to be done.")
        parser.add_argument("--teams", action="store_true",
                            help="Use with --show, also list all the rack's servers grouped by teams.")
        parser.add_argument("--downtime", action="store_true",
                            help='Depool: downtime all servers for 4h; Pool: remove the downtime.')
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return DepoolRackRunner(args, self.spicerack)


class DepoolRackRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    def __init__(self, args, spicerack):
        """Initialize the rack depool runner."""
        self.args = args
        self.netbox = spicerack.netbox()
        self.puppetserver = spicerack.puppet_server()
        self.remote = spicerack.remote()
        self.dry_run = spicerack.dry_run
        self.run_cookbook = spicerack.run_cookbook
        # Store if a k8s node is in the rack for special treatment
        self.k8s = False

        # Get all active servers from the given rack
        # Possible improvements:
        # * Move it to the netbox spicerack module instead of direct API calls
        # * Get the list of servers connected to the ToR instead of all the servers
        rack = self.netbox.api.dcim.racks.get(site=self.args.site, name=self.args.rack)
        if not rack:
            raise RuntimeError(f"Can't find {self.args.site} rack {self.args.rack} in Netbox")
        hostnames = list(self.netbox.api.dcim.devices.filter(role='server', rack_id=rack.id, status='active'))
        netbox_servers = [spicerack.netbox_server(str(hostname)) for hostname in hostnames]
        self.definitions = self.fetch_hiera_definitions(netbox_servers)

        # If we use --show or --teams, it stops here
        if self.args.show or self.args.teams:
            raise CookbookInitSuccess()

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f"with action '{self.args.action}' for {self.args.site} rack {self.args.rack}"

    def run(self):
        """Main entry point, as required by Spicerack API."""
        ask_confirmation(f"Proceed to {self.args.action} {self.args.site} rack {self.args.rack}?")
        if self.args.downtime:
            self.downtime()
        self.run_actions(self.definitions)
        if self.k8s:
            self.run_k8s_pool_depool_node()

    def run_k8s_pool_depool_node(self) -> None:
        """Call the sre.k8s.pool-depool-node cookbook."""
        k8s_clusters_prefixes = ('staging', 'ml-staging', 'aux', 'ml-serve', 'dse', 'wikikube')
        for prefix in k8s_clusters_prefixes:
            cookbook_args = ['--k8s-cluster', f'{prefix}-{self.args.site}',
                             self.args.action,  # TODO add --force once the cookbook is more trusted
                             '--rack', self.args.rack]
            ask_confirmation(f'Proceed to run sre.k8s.pool-depool-node {" ".join(cookbook_args)} ?')
            ret_val = self.run_cookbook('sre.k8s.pool-depool-node', cookbook_args)
            if ret_val != 0:
                logger.error("cookbook 'sre.k8s.pool-depool-node %s' didn't run successfully", " ".join(cookbook_args))

    def downtime(self) -> None:
        """Manage all the rack's hosts downtime."""
        if self.args.action == 'pool':
            logger.info("Removing downtime is not supported.")
            return
        cookbook_args = ['-r', f'"Rack {self.args.rack} {self.args.action}"',
                         '-H', '4',
                         f"'P{{P:netbox::host%location ~ \"{self.args.rack}.*{self.args.site}\"}}'"]
        ask_confirmation(f'Proceed to run sre.hosts.downtime {" ".join(map(str, cookbook_args))} ?')
        ret_val = self.run_cookbook('sre.hosts.downtime', cookbook_args)
        if ret_val != 0:
            logger.error("cookbook 'sre.hosts.downtime %s' didn't run successfully", " ".join(map(str, cookbook_args)))
            ask_confirmation('You probably want to run it manually. Continue ?')
        return

    def run_actions(self, definitions) -> None:
        """Run the actions to mass pool or depool the given servers."""
        for netbox_server, policy_command in definitions.items():
            if policy_command['policy'] == 'k8s':
                logger.info("%s: will be tackled at the end with the k8s cookbook", netbox_server.name)
                continue
            logger.info("%s: running %s '%s'",
                        netbox_server.name,
                        policy_command['policy'],
                        policy_command['command'])
            ask_confirmation('Proceed ?')
            if policy_command['policy'] == 'local_command':
                remote_host = self.remote.query(netbox_server.fqdn)
                try:
                    remote_host.run_sync(policy_command['command'], print_progress_bars=False)
                except RemoteExecutionError:
                    ask_confirmation('Error while running the command, continue to the next host?')
            if policy_command['policy'] == 'cookbook':
                command_with_host = policy_command['command']
                # Replace a potential '{fqdn}' or '{host}' placeholder with the needed device identifier.
                for identifier_type in ('name', 'fqdn'):
                    if '{' + identifier_type + '}' in policy_command['command']:
                        command_with_host = policy_command['command'].format_map(
                            {identifier_type: netbox_server.getattr(identifier_type)})
                        break
                cookbook_name = command_with_host.split(' ')[0]
                if '.' not in cookbook_name:  # Safeguard
                    logger.error("%s: skipping host (invalid cookbook name '%s')", netbox_server.name, cookbook_name)
                    continue
                cookbook_args = command_with_host.split(' ')[1:]
                ret_val = self.run_cookbook(cookbook_name, cookbook_args)
                if ret_val != 0:
                    logger.error("%s: cookbook '%s' didn't run successfully", netbox_server.name, command_with_host)

    def fetch_hiera_definitions(self, netbox_servers) -> dict[NetboxServer, dict]:
        """Fetch the pool or depool policies and commands from Hiera for each server."""
        definitions: dict[NetboxServer, dict] = {}
        contacts_servers = defaultdict(list)
        logger.info("The cookbook will now try to render and inspect each host's Hiera def."
                    " This can take up to 15min to finish.")
        for netbox_server in netbox_servers:
            try:
                role_contacts: list = json.loads(
                    self.puppetserver.hiera_lookup(netbox_server.fqdn, "profile::contacts::role_contacts", fmt="json"))
                for role_contact in role_contacts:
                    contacts_servers[role_contact].append(netbox_server.name)
                hiera_data = json.loads(
                    self.puppetserver.hiera_lookup(netbox_server.fqdn,
                                                   f"profile::server_{self.args.action}",
                                                   fmt="json"))
            except (ValueError, json.JSONDecodeError, StopIteration, RemoteExecutionError):
                logger.info("%s: Couldn't get or parse %s Hiera key", netbox_server.name, self.args.action)
                continue
            if 'policy' not in hiera_data or hiera_data['policy'] == "skip" or not hiera_data['policy']:
                logger.info("%s: skipping host (%s)",
                            netbox_server.name,
                            hiera_data.get('message', 'no depool needed'))
                continue
            if hiera_data['policy'] == 'k8s':
                self.k8s = True
            elif hiera_data['policy'] == 'zarcillo':
                zarcillo_response = _query_zarcillo(netbox_server.name)
                if not zarcillo_response:
                    logger.info("%s: Couldn't query Zarcillo, please check manually", netbox_server.name)
                    continue
                if zarcillo_response['can_depool']:
                    hiera_data['policy'] = 'cookbook'
                    hiera_data['command'] = f'sre.mysql.{self.args.action} -r "rack {self.args.action}" {{name}}'
                else:
                    logger.info("%s: skipping host (manual %s needed)",
                                netbox_server.name,
                                self.args.action)
                    continue

            logger.info("%s: %s using %s %s",
                        netbox_server.name,
                        self.args.action,
                        hiera_data['policy'],
                        hiera_data.get('command', ''))

            definitions[netbox_server] = hiera_data

        if self.args.teams:
            for contact, servers in contacts_servers.items():
                logger.info("%s: %s", contact, ', '.join(servers))

        return definitions


def _query_zarcillo(host: str) -> dict:
    """Fetch json dict"""
    try:
        with urlopen(f'https://zarcillo.wikimedia.org/api/v1/can_be_depooled/{host}', timeout=5) as resp:
            return json.loads(resp.read())
    except Exception as e:  # pylint: disable=broad-except
        logger.info("%s: error while querying Zarcillo: %s", host, e)
        return {}
