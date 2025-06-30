"""DNS Discovery Operations"""
import logging
import time

from spicerack import Spicerack
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs
from spicerack.confctl import ConfctlError
from wmflib.constants import CORE_DATACENTERS

from cookbooks.sre.discovery import check_record_for_dc, update_ttl

logger = logging.getLogger(__name__)

# Fixme: Move to spicerack.constants
# DNS_TTL_LONG = 3600
DNS_TTL_MEDIUM = 300
DNS_TTL_SHORT = 10


class DiscoveryServiceRoute(CookbookBase):
    """Pool/Depool/Check services via DNS Discovery operations.

    This cookbook automates DNS Discovery operations like pool and depool of
    specific services.

    Examples:
    - Check the state of the test-svc service:
      cookbook sre.discovery.service-route check test-svc

    - Depool a list of services from codfw:
      cookbook sre.discovery.service-route depool codfw test-svc test-svc2

    - Pool a list of services from codfw:
      cookbook sre.discovery.service-route pool codfw test-svc test-svc2

    - Depool a service from codfw and wipe the DNS recursors' cache:
      cookbook sre.discovery.service-route pool codfw test-svc test-svc2 --wipe-cache

      Please note: this cookbook does not handle active/active vs active/passive
      distinctions, it will just execute a pool/depool action as the operator
      says. It currently does not prevent you from doing unwanted actions like
      depooling the only active DC of an active/passive svc.

    """

    argument_reason_required = False

    def argument_parser(self):
        """Parse the command line arguments for all the sre.discovery cookbooks."""
        parser = super().argument_parser()
        actions = parser.add_subparsers(dest='action', help='The action to perform')
        action_check = actions.add_parser('check')
        action_check.add_argument('services', nargs='+', help='The services to operate on')
        action_pool = actions.add_parser('pool')
        action_depool = actions.add_parser('depool')

        for a in (action_pool, action_depool):
            a.add_argument('datacenter', choices=CORE_DATACENTERS, help='Name of the datacenter. One of: %(choices)s.')
            a.add_argument('services', nargs='+', help='The services to operate on')
            a.add_argument('--wipe-cache', action='store_true', help='Wipe the cache on DNS recursors.')

        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return DiscoveryServiceRouteRunner(args, self.spicerack)


class DiscoveryServiceRouteRunner(CookbookRunnerBase):
    """Pool/Depool/Check services via DNS Discovery operations runner class."""

    def __init__(self, args, spicerack: Spicerack):
        """Initialize the runner."""
        self.args = args
        self.spicerack = spicerack
        dnsdisc = self.spicerack.discovery(*self.args.services)

        try:
            self.active_dcs = dnsdisc.active_datacenters
        except ConfctlError as e:
            logger.error('dnsdisc %s: %s', self.args.services, e)
            raise RuntimeError(
                "An error has occurrend while the cookbook was running its init steps"
            ) from e

        self.action_services = []
        self.depool = self.args.action == 'depool'
        self.pool = self.args.action == 'pool'
        try:
            for service, self.active_dcs in dnsdisc.active_datacenters.items():
                if self.pool and self.args.datacenter not in self.active_dcs:
                    # This services needs to be pooled in args.datacenter
                    self.action_services.append(service)
                elif self.depool and self.args.datacenter in self.active_dcs:
                    # This service needs to be depooled in args.datacenter
                    self.action_services.append(service)
        except ConfctlError as e:
            logger.error('dnsdisc %s: %s', self.args.services, e)
            raise RuntimeError(
                "An error has occurrend while the cookbook was running its init steps"
            ) from e

        self.dnsdisc = spicerack.discovery(*self.action_services)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        if len(self.args.services) == 1:
            services_msg = self.args.services[0]
        else:
            services_msg = f'{len(self.args.services)} services'

        reason = self.args.reason if self.args.reason else "maintenance"
        if self.args.action == "check":
            log_msg = f"{self.args.action} {services_msg}: {reason}"
        else:
            log_msg = f"{self.args.action} {services_msg} in {self.args.datacenter}: {reason}"
        return log_msg

    @property
    def lock_args(self):
        """Customize the lock arguments."""
        if self.args.action == 'check':
            suffix = "ro"
            concurrency = CookbookRunnerBase.max_concurrency
            ttl = 60
        else:
            suffix = "rw"
            concurrency = 1
            ttl = 600

        return LockArgs(suffix=suffix, concurrency=concurrency, ttl=ttl)

    def check(self):
        """Check the current state of the service in conftool and on authoritative DNS servers."""
        print('Expected routes:')
        for svc in self.args.services:
            svc_active_dcs = self.dnsdisc.active_datacenters.get(svc, [])
            route = ','.join(sorted(svc_active_dcs))
            print('{service}: {route}'.format(service=svc, route=route))
            for dc in CORE_DATACENTERS:
                dc_to = dc
                if dc not in svc_active_dcs:
                    # If DC is not active, the discovery record should point to the other DC
                    # WARNING: This assumes we only have two datacenters.
                    dc_to = svc_active_dcs[0]

                # Not all discovery records have a <svc>.<dc>.wmnet record.
                # For instance, appservers-{rw,ro} will need to resolve appserver.svc.<dc>.wmnet.
                # WARNING: This is a hard coded assumption that may not cover all cases correctly.
                svc_to = svc
                for postfix in ('-rw', '-ro', '-async', '-php'):
                    if svc_to.endswith(postfix):
                        svc_to = svc_to[:-len(postfix)]

                expected_name_fmt = '{service}.svc.{dc_to}.wmnet'
                if svc_to != svc:
                    logger.info('Stripped prefix from expected target service name: %s -> %s',
                                expected_name_fmt.format(service=svc, dc_to=dc_to),
                                expected_name_fmt.format(service=svc_to, dc_to=dc_to))

                # Check if authdns reflects the conftool/etcd setting
                check_record_for_dc(self.spicerack.dry_run, self.dnsdisc, dc, svc,
                                    expected_name_fmt.format(service=svc_to, dc_to=dc_to))
        return 0

    def pool_or_depool(self):
        """Pool/Depool services from given datacenters."""
        old_ttl = update_ttl(self.dnsdisc, DNS_TTL_MEDIUM)
        if self.pool:
            self.dnsdisc.pool(self.args.datacenter)
        elif self.depool:
            self.dnsdisc.depool(self.args.datacenter)

        # The actual work is done now. DNS should be propagated in old_ttl seconds from now at the latest.
        records_propagated_at = time.time() + old_ttl

        if self.args.wipe_cache:
            records = ' '.join([f'{service}.discovery.wmnet' for service in self.action_services])
            wipe_ret = self.spicerack.run_cookbook('sre.dns.wipe-cache', [records])
            if wipe_ret:
                logger.warning('Failed to wipe the DNS recursors caches for records: %s', records)

        sleep_time = records_propagated_at - time.time()
        if sleep_time > 0:
            logger.info('Waiting %.2f seconds for DNS changes to propagate', sleep_time)
            if not self.spicerack.dry_run:
                time.sleep(sleep_time)

        # This just checks auth servers
        # FIXME: Check the availability of the records on the resolvers as well?
        return self.check()

    def run(self):
        """Required by Spicerack API."""
        # Exit early if no changes where necessary
        if len(self.action_services) == 0:
            logger.info('All services are already in the desired state')
        else:
            if self.args.action == 'check':
                self.check()
            else:
                self.pool_or_depool()
