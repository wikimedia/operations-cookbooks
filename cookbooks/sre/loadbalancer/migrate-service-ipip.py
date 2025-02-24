"""Cookbook to migrate services to IPIP encapsulation"""

from datetime import timedelta
from logging import getLogger
from socket import getfqdn, socket, AF_INET, SOCK_STREAM
from textwrap import dedent

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.remote import RemoteError
from wmflib.constants import CORE_DATACENTERS
from wmflib.dns import DnsNotFound
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

# pylint: disable=no-name-in-module
from scapy.all import IP, L3RawSocket, TCP, sr1  # type: ignore[attr-defined]
from scapy.all import conf as scapyconf
# pylint: enable=no-name-in-module


class MigrateServiceIPIP(CookbookBase):
    """Migrate existing LVS services to IPIP

    Performed steps:
    1. Asks the user to perform the required hiera changes
    2. Runs puppet on LVS and realservers
    3. Validates that realservers are able to handle incoming IPIP traffic
    4. Restarts pybal on affected loadbalancers

    Usage:
        cookbook sre.loadbalancer.migrate-service-ipip --dc codfw --role swift::proxy swift swift-https
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--dc",
            type=str,
            required=True,
            choices=CORE_DATACENTERS,
            help="Target datacenter. One of %(choices)s.",
        )
        parser.add_argument(
            "--role",
            type=str,
            required=True,
            help="Puppet role used by the realservers",
        )
        parser.add_argument(
            "services",
            nargs="+",
            help="Service(s) to be migrated",
        )
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return MigrateServiceIPIPRunner(args, self.spicerack)


class MigrateServiceIPIPRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    IPIP_OUTER_SRC_IP = "172.16.1.1"

    def __init__(self, args, spicerack):
        """Initiliaze the MigrateServiceIPIPRunner runner."""
        scapyconf.sniff_promisc = False
        scapyconf.L3socket = L3RawSocket
        # https://github.com/secdev/scapy/issues/383
        scapyconf.checkIPinIP = False

        ensure_shell_is_durable()
        self.spicerack = spicerack
        self.dry_run = spicerack.dry_run
        self.role = args.role
        self.dc = args.dc
        self.fqdn = getfqdn()
        self.dns = spicerack.dns()
        catalog = spicerack.service_catalog()
        self.services = [
            service
            for service in catalog
            if service.name in args.services
            and service.lvs is not None
            and service.ip.get(self.dc) is not None
        ]
        if len(self.services) == 0:
            raise RuntimeError(f"No services found matching {args.services} and LVS configured")

        self.logger = getLogger(__name__)
        realservers_query = f"P{{O:{self.role}}} and A:{self.dc}"
        try:
            self.realservers_remote_hosts = spicerack.remote().query(realservers_query)
        except RemoteError as error:
            raise RuntimeError("No hosts found matching {self.role} in {self.dc}") from error

        self.realservers_puppet = spicerack.puppet(self.realservers_remote_hosts)

        self.lvs_query = f"(A:lvs-low-traffic-{self.dc} or A:lvs-secondary-{self.dc}) and A:bullseye"
        try:
            self.lvs_remote_hosts = spicerack.remote().query(self.lvs_query)
        except RemoteError as error:
            raise RuntimeError("No LVS found in {self.dc}") from error

        self.lvs_puppet = spicerack.puppet(self.lvs_remote_hosts)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return f'for role: {self.role}@{self.dc}'

    def _ipip_traffic_accepted(self,  *,
                               outer_src_ip: str, outer_dst_ip: str,
                               inner_src_ip: str, inner_dst_ip: str,
                               dport: int) -> bool:
        """Send a single SYN packet using IPIP encapsulation"""
        s = socket(AF_INET, SOCK_STREAM)
        s.bind((inner_src_ip, 0))
        sport = s.getsockname()[1]
        syn_packet = (
            IP(src=outer_src_ip, dst=outer_dst_ip) /
            IP(src=inner_src_ip, dst=inner_dst_ip) /
            TCP(sport=sport, dport=dport, flags="S", seq=1000)
        )
        response = sr1(syn_packet, timeout=3, verbose=self.dry_run)
        s.close()
        return response is not None

    def _resolve_ipv4(self, hostname: str) -> str:
        try:
            addresess = self.dns.resolve_ipv4(hostname)
        except DnsNotFound as e:
            raise RuntimeError(f"unable to resolve {hostname}") from e

        if len(addresess) > 0:
            return addresess[0]

        raise RuntimeError(f"unable to resolve {hostname}")

    @retry(backoff_mode='constant', delay=timedelta(seconds=1), exceptions=(RuntimeError,))
    def ask_update_hiera(self):
        """Inform the user to manually update the hiera config and check this has been performed."""
        ask_confirmation(dedent(
            f"""\
            Please add the following hiera entry to:
            hieradata/role/{self.dc}/{self.role.replace('::', '/')}.yaml
                profile::lvs::realserver::ipip::enabled: true
                profile::base::enable_rp_filter: false

            Please update the target service(s) definition
            located in hieradata/common/service.yaml and include under the lvs key:
                scheduler: mh
                scheduler_flag: mh-port
                ipip_encapsulation:
                - {self.dc}

            Press continue when the change is merged
            """
        ))
        self.realservers_puppet.run()
        self.lvs_puppet.run()

        inner_src_ip = self._resolve_ipv4(self.fqdn)
        for realserver_host in self.realservers_remote_hosts.hosts:
            outer_dst_ip = self._resolve_ipv4(realserver_host)
            for service in self.services:
                inner_dst_ip = str(service.ip.get(self.dc))
                if not self._ipip_traffic_accepted(outer_src_ip=self.IPIP_OUTER_SRC_IP,
                                                   outer_dst_ip=outer_dst_ip,
                                                   inner_src_ip=inner_src_ip,
                                                   inner_dst_ip=inner_dst_ip, dport=service.port):
                    raise RuntimeError(f"""{realserver_host} is not accepting incoming IPIP traffic:
                                        outer IP header: {self.IPIP_OUTER_SRC_IP} -> {outer_dst_ip}
                                        inner IP header: {inner_src_ip} -> {inner_dst_ip}
                                        destination port: {service.port}
                                       """)

    def restart_pybal(self) -> int:
        """Trigger sre.loadbalancer.restart-pybal cookbook"""
        ask_confirmation("Press continue when you are ready to restart pybal.")
        args = ("--query", self.lvs_query, "--reason", f"migrating {self.role}@{self.dc} to IPIP encapsulation")
        return self.spicerack.run_cookbook("sre.loadbalancer.restart-pybal", args)

    def rollback(self):
        """Rollback actions."""
        print("The cookbook has failed you will need to manually investigate the state.")

    def run(self):
        """Main run method either query or clear MigrateHosts events."""
        self.ask_update_hiera()
        return self.restart_pybal()
