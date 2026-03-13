"""Cookbook to validate that realservers can handle IPIP/IP6IP6 traffic"""
import logging
import random

from datetime import timedelta
from logging import getLogger
from socket import getfqdn

from spicerack.cookbook import CookbookBase, CookbookInitSuccess, CookbookRunnerBase
from spicerack.decorators import retry
from spicerack.remote import RemoteError
from wmflib.constants import ALL_DATACENTERS
from wmflib.dns import DnsNotFound

# pylint: disable=no-name-in-module
from scapy.all import IP, IPv6, TCP, sr1  # type: ignore[attr-defined]
from scapy.all import conf as scapyconf
# pylint: enable=no-name-in-module


logger = logging.getLogger(__name__)


class CheckIPIP(CookbookBase):
    """Validate that realservers are able to handle IPIP traffic

    Usage:
        cookbook sre.loadbalancer.check-ipip --dc codfw --query A:ncredir ncredir ncredir-https
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument(
            "--dc",
            type=str,
            required=True,
            choices=ALL_DATACENTERS,
            help="Target datacenter. One of %(choices)s.",
        )
        parser.add_argument(
            "--query",
            type=str,
            help="Query used to match realservers using global grammar (e.g. `P{foo100[5-7]*}`)",
        )
        parser.add_argument(
            "services",
            nargs="+",
            help="Service(s) to be checked",
        )
        return parser

    def get_runner(self, args):
        """As required by Spicerack API."""
        return CheckIPIPRunner(args, self.spicerack)


class CheckIPIPRunner(CookbookRunnerBase):
    """As required by Spicerack API."""

    # pylint: disable=too-many-instance-attributes

    IPIP_OUTER_SRC_IP = "172.16.1.1"

    def __init__(self, args, spicerack):
        """Initiliaze the MigrateServiceIPIPRunner runner."""
        scapyconf.sniff_promisc = False
        # https://github.com/secdev/scapy/issues/383
        scapyconf.checkIPinIP = False

        self.spicerack = spicerack
        self.query = args.query
        self.dc = args.dc
        self.fqdn = getfqdn()
        self.dns = spicerack.dns()
        catalog = spicerack.service_catalog()
        self.services = [
            service
            for service in catalog
            if service.name in args.services
            and service.lvs is not None
            and self.dc in service.ip.data
        ]
        if len(self.services) == 0:
            raise RuntimeError(f"No services found matching {args.services} and LVS configured")

        self.logger = getLogger(__name__)
        try:
            query = f"{args.query} and A:{self.dc}"
            self.realservers_remote_hosts = spicerack.remote().query(query)
        except RemoteError as error:
            raise RuntimeError(f"No hosts found matching {self.query} in {self.dc}") from error

        # used to track validated realservers and avoid retrying them
        self._validated_realservers: set[str] = set()
        self.validate_realservers()
        raise CookbookInitSuccess

    def _ipip_traffic_accepted(self,  *,
                               outer_src_ip: str, outer_dst_ip: str,
                               inner_src_ip: str, inner_dst_ip: str,
                               dport: int) -> bool:
        """Send a single SYN packet using IPIP/IP6IP6 encapsulation"""
        if ':' in inner_dst_ip:
            L3 = IPv6
        else:
            L3 = IP

        sport = random.randint(1024, 65535)  # nosec B311
        syn_packet = (
            L3(src=outer_src_ip, dst=outer_dst_ip) /
            L3(src=inner_src_ip, dst=inner_dst_ip) /
            TCP(sport=sport, dport=dport, flags="S", seq=1000, options=[('MSS', 1400)])
        )
        response = sr1(syn_packet, timeout=3)
        return response is not None

    def _resolve_ips(self, hostname: str) -> list[str]:
        try:
            return self.dns.resolve_ips(hostname)
        except DnsNotFound as e:
            raise RuntimeError(f"unable to resolve {hostname}") from e

    @retry(backoff_mode='constant', delay=timedelta(seconds=1), exceptions=(RuntimeError,))
    def validate_realservers(self):
        """Check that realservers accept inbound IPIP traffic for the configured services"""
        inner_ips = self._resolve_ips(self.fqdn)
        inner_ipv4 = next((addr for addr in inner_ips if ':' not in addr), None)
        if inner_ipv4 is None:
            raise RuntimeError("unable to detect local IPv4 address")
        inner_ipv6 = next((addr for addr in inner_ips if ':' in addr), None)
        if inner_ipv6 is None:
            raise RuntimeError("unable to detect local IPv6 address")

        for realserver_host in self.realservers_remote_hosts.hosts:
            if str(realserver_host) in self._validated_realservers:
                logger.debug("Skipping already validated realserver: %s", realserver_host)
                continue

            realserver_ips = self._resolve_ips(realserver_host)
            for service in self.services:
                for addr in service.ip.data[self.dc].values():
                    inner_dst_ip = str(addr)
                    if ':' in inner_dst_ip:
                        outer_dst_ip = next((addr for addr in realserver_ips if ':' in addr), None)
                        if outer_dst_ip is None:
                            raise RuntimeError(f"""service {service.name} supports IPv6
                                                but realserver {realserver_host} doesn't""")
                        inner_src_ip = inner_ipv6
                        outer_src_ip = "0100::1"
                    else:
                        outer_dst_ip = next((addr for addr in realserver_ips if ':' not in addr), None)
                        if outer_dst_ip is None:
                            raise RuntimeError(f"""service {service.name} supports IPv4
                                                but realserver {realserver_host} doesn't""")
                        inner_src_ip = inner_ipv4
                        outer_src_ip = "172.16.1.1"

                    logger.info("Validating that realserver %s (%s) accepts IPIP traffic for %s (%s)",
                                realserver_host, outer_dst_ip, service.name, inner_dst_ip)

                    if not self._ipip_traffic_accepted(outer_src_ip=outer_src_ip,
                                                       outer_dst_ip=outer_dst_ip,
                                                       inner_src_ip=inner_src_ip,
                                                       inner_dst_ip=inner_dst_ip, dport=service.port):
                        raise RuntimeError(f"""{realserver_host} is not accepting incoming IPIP traffic:
                                            outer IP header: {outer_src_ip} -> {outer_dst_ip}
                                            inner IP header: {inner_src_ip} -> {inner_dst_ip}
                                            destination port: {service.port}
                                           """)
            # All services validated to this host, mark it as done
            self._validated_realservers.add(str(realserver_host))

    def run(self):
        """Main run method."""
