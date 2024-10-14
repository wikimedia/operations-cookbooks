"""Move a server from the old VLAN schema to the new one."""
import logging

from ipaddress import ip_interface


from wmflib.interactive import ask_confirmation, confirm_on_failure, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs

from cookbooks.sre.hosts import (
    check_patterns_in_repo,
    get_grep_patterns,
    GitRepoPath,
    DEPLOYMENT_HOST,
    MEDIAWIKI_CONFIG_REPO_PATH,
    KERBEROS_KADMIN_CUMIN_ALIAS,
    PUPPETSERVER_REPO_PATH,
    PUPPETSERVER_PRIVATE_REPO_PATH,
    DEPLOYMENT_CHARTS_REPO_PATH,
    AUTHDNS_REPO_PATH,
    LEGACY_VLANS
)

logger = logging.getLogger(__name__)


class MoveVlan(CookbookBase):
    """Move a server from the old VLAN schema to the new one, changing its IP addresses.

    Actions performed:
        * Checks that the server is in a state to be re-numbered
        * Update Netbox with the new networking details
        * Update the switch port config
        * Update DNS
        * Wipe DNS recursors cache

    Usage:
        cookbook sre.hosts.move-vlan preflight example1001
        cookbook sre.hosts.move-vlan reimage example1001

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('action', choices=('preflight', 'reimage', 'inplace'),
                            help='Action to perform. The reimage one is meant to be called by the reimage cookbook.')
        parser.add_argument('host', help='Short hostname of the host to move, not FQDN')
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return MoveVlanRunner(args, self.spicerack)


class MoveVlanRunner(CookbookRunnerBase):  # pylint: disable=too-many-instance-attributes
    """As required by Spicerack API."""

    @property
    def runtime_description(self):
        """Runtime description for the IRC/SAL logging."""
        return f'for host {self.netbox_server.name}'

    def __init__(self, args, spicerack):
        """Initiliaze the move vlan runner."""
        ensure_shell_is_durable()
        self.args = args

        self.spicerack = spicerack
        self.dns = spicerack.dns()
        self.netbox = spicerack.netbox(read_write=True)
        self.remote = spicerack.remote()

        self.netbox_server = spicerack.netbox_server(self.args.host, read_write=True)
        self.netbox_data = self.netbox_server.as_dict()
        self.netbox_host = self.netbox.api.dcim.devices.get(self.netbox_data['id'])
        self.alerting_hosts = self.spicerack.alerting_hosts([self.args.host])
        self.puppet_server = self.spicerack.puppet_server().server_host
        self.kerberos_kadmin = self.remote.query(KERBEROS_KADMIN_CUMIN_ALIAS)
        self.deployment_host = self.remote.query(self.dns.resolve_cname(DEPLOYMENT_HOST))
        self.authdns_hosts = spicerack.authdns_active_hosts
        self.patterns = get_grep_patterns(self.dns, [self.netbox_server.fqdn], ip_only=True)
        self.post_config = {}
        self.pre_config = {4: self.netbox_server.primary_ip4_address,
                           'vlan': self.netbox_server.access_vlan}

        if self.netbox_server.primary_ip6_address:
            self.pre_config[6] = self.netbox_server.primary_ip6_address

        if self.netbox_host.primary_ip.assigned_object:
            self.pre_config['count_ipaddresses'] = self.netbox_host.primary_ip.assigned_object.count_ipaddresses

        self.reason = self.spicerack.admin_reason('Migrating vlan and changing IPs')

        # Keep track of some specific actions for the eventual rollback
        self.rollback_netbox = False
        self.dns_propagated = False
        self.switch_port_configured = False

        # Always run the pre flight checks at each run
        if not self.pre_flight():
            raise RuntimeError('The host is not suitable for the migration, see above.')

        if self.args.action == 'inplace':
            raise NotImplementedError('In-place migration not yet available.')

    def pre_flight(self) -> bool:
        """Check if there are any blockers to the migration."""
        if self.netbox_server.virtual:
            logger.info('This is only for physical servers, nothing to do. 👍')
            return False
        if self.pre_config['vlan'] not in LEGACY_VLANS:
            logger.info('Server not in a vlan requiring a migration, nothing to do. 👍')
            return False

        if ((self.pre_config['count_ipaddresses'] > 2 and self.netbox_server.primary_ip4_address) or
           (self.pre_config['count_ipaddresses'] > 1 and not self.netbox_server.primary_ip4_address)):
            logger.info('Too many IPs configure on the primary interface, manual migration required.')
            return False

        # Does it have hardcoded IPs (v4 or v6) in any repositories ?
        found_pattern = check_patterns_in_repo(
            (
                GitRepoPath(
                    remote_host=self.puppet_server, path=PUPPETSERVER_REPO_PATH, pathspec=":!manifests/site.pp"
                ),
                GitRepoPath(remote_host=self.puppet_server, path=PUPPETSERVER_PRIVATE_REPO_PATH),
                GitRepoPath(remote_host=self.deployment_host, path=MEDIAWIKI_CONFIG_REPO_PATH),
                GitRepoPath(remote_host=self.deployment_host, path=DEPLOYMENT_CHARTS_REPO_PATH),
                GitRepoPath(remote_host=self.authdns_hosts, path=AUTHDNS_REPO_PATH),
            ),
            self.patterns,
            interactive=False,
        )
        if found_pattern:
            # not retuning False as it's informational and not a blocker
            logger.info('Hardcoded IPs found in the repos, manual additions will be needed during the migration.')

        return True

    def generate_new_ip_vlan(self) -> dict:
        """Get the future vlan and IPs based on the current vlan."""
        vlan_type = self.pre_config['vlan'].split('1')[0]  # works with private1 but not any potential private2
        new_rack_name = self.netbox_data["rack"]["name"].lower()
        site_name = self.netbox_data["site"]["slug"]
        new_vlan_name = f'{vlan_type}1-{new_rack_name}-{site_name}'

        new_vlan = self.netbox.api.ipam.vlans.get(name=new_vlan_name, status='active')
        if not new_vlan:
            raise RuntimeError(f"Failed to find active VLAN with name {new_vlan_name} in Netbox.")

        prefix_v4 = self.netbox.api.ipam.prefixes.get(vlan_id=new_vlan.id, family=4)
        # TODO: This step has a race with the provision script in Netbox and any run of this cookbook!
        # See https://phabricator.wikimedia.org/T365694 for long term fix
        new_v4_ip = ip_interface(prefix_v4.available_ips.list()[0]['address'])

        primary_ip6 = self.netbox_server.primary_ip6_address
        new_v6_ip = None
        # If there is already a primary_ip6 that means we need to generate a new one
        if primary_ip6:
            prefix_v6 = self.netbox.api.ipam.prefixes.get(vlan_id=new_vlan.id, family=6)
            prefix_v6_base, prefix_v6_mask = str(prefix_v6).split("/")
            # Generate the IPv6 address embedding the IPv4 address, for example from an IPv4 address 10.0.0.1 and an
            # IPv6 prefix 2001:db8:3c4d:15::/64 the mapped IPv6 address 2001:db8:3c4d:15:10:0:0:1/64 is generated.
            mapped_v4 = str(new_v4_ip).split('/', maxsplit=1)[0].replace(".", ":")
            prefix_v6 = prefix_v6_base.rstrip(':')
            new_v6_ip = ip_interface(f'{prefix_v6}:{mapped_v4}/{prefix_v6_mask}')

        return {4: new_v4_ip, 6: new_v6_ip, 'vlan': new_vlan_name}

    def update_netbox_ip_vlan(self, config: dict):
        """Update IPs and/or access vlan config of the host."""
        if self.spicerack.dry_run:
            logger.info('Would have updated Netbox with %s', config)
            return

        if config['vlan']:
            self.netbox_server.access_vlan = config['vlan']
            logger.info('Updated switchport access vlan to %s', config['vlan'])

        if config[4]:
            self.netbox_server.primary_ip4_address = config[4]
            logger.info('Updated IPv4 to %s', config[4])

        if config[6]:
            self.netbox_server.primary_ip6_address = config[6]
            logger.info('Updated IPv6 to %s', config[6])

    def run_raise(self, name: str, args: list):
        """Run a cookbook and raise an error if return code is non-zero."""
        ret = self.spicerack.run_cookbook(name, args)
        if ret:
            raise RuntimeError(f'Failed to run cookbook {name}')

    def propagate_dns(self, action: str, config: dict):
        """Run the sre.dns.netbox cookbook to propagate the DNS records."""
        confirm_on_failure(self.run_raise, 'sre.dns.netbox', [f'{action} records for host {self.args.host}'])
        self.dns_propagated = True
        # Clean out DNS cache to remove stale NXDOMAINs
        ptr4 = config[4].ip.reverse_pointer
        records_to_wipe = [self.netbox_server.fqdn, ptr4]
        if config[6]:
            records_to_wipe.append(config[6].ip.reverse_pointer)

        confirm_on_failure(self.run_raise, 'sre.dns.wipe-cache', records_to_wipe)

    def run(self):
        """Run the cookbook."""
        # If only the checks were needed, stop here
        if self.args.action == 'preflight':
            return

        self.post_config = self.generate_new_ip_vlan()
        self.update_netbox_ip_vlan(self.post_config)
        self.rollback_netbox = True

        ask_confirmation('At this point, add the new IPs in the repositories listed previously (if any). Continue?')
        # Pass the pre_config to clear the now unused PTR records
        self.propagate_dns('Update', self.pre_config)

        if self.args.action == 'reimage':
            # Run the sre.network.configure-switch-interfaces cookbook
            logger.info('Updating the switch port config, the host will lose connectivity.')
            confirm_on_failure(self.run_raise, 'sre.network.configure-switch-interfaces', [self.args.host])
            # At this point, if the re-image fails, it's better to rollforward and run the re-image cookbook
            self.rollback_netbox = False

        logger.info("All done, don't forget to remove the old IPs references in the repos (if any).")

    @property
    def lock_args(self):
        """Make the cookbook lock per-rack."""
        suffix = f'{self.netbox_data["site"]["slug"]}-{self.netbox_data["rack"]["name"]}'
        return LockArgs(suffix=suffix, concurrency=1, ttl=3600)

    def rollback(self):
        """Rollback the various changes depending on the process advancements on failure."""
        if not self.rollback_netbox:
            logger.info("Nothing to rollback. 👍")
            return

        # Workaround bug https://github.com/netbox-community/pynetbox/issues/586
        # by refreshing netbox_server, otherwise the IPs/vlan are not rolled back despite Spicerack saying so
        self.netbox_server = self.spicerack.netbox_server(self.netbox_data['name'], read_write=True)
        # Low risk of previous IPs being re-allocated before the rollback
        # as new hosts should not go in the old vlans
        self.update_netbox_ip_vlan(self.pre_config)
        # Keeping it here as it will be needed for the in-place re-numbering
        if self.switch_port_configured:
            confirm_on_failure(self.run_raise, 'sre.network.configure-switch-interfaces', [self.args.host])

        if self.dns_propagated:
            self.propagate_dns('Rollback', self.post_config)

        logger.info("Rollback completed. 👍")
