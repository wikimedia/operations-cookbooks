"""Reboot all machines in a cluster.

For groups of machines in the cluster it will:
- Depool all machines
- Set Icinga downtime
- Reboot
- Wait for host to come back online
- Remove the Icinga downtime after the host has been rebooted, the
  first Puppet run is complete and (optionally) icinga is all green.
- Repool all machines


Usage example:
    cookbook sre.hosts.reboot-cluster -D eqiad -c api_appserver -p 5 -s 45.0

This command will cause a rolling reboot of the nodes in the api_appserver
conftool cluster, 5% at a time, waiting 45 seconds before rebooting.
"""
import argparse
import logging
import math
import time

from datetime import datetime, timedelta
from typing import List

import attr

from cookbooks import ArgparseFormatter
from cumin import NodeSet
from spicerack.constants import CORE_DATACENTERS
from spicerack.decorators import retry
from spicerack.icinga import IcingaError
from spicerack.puppet import PuppetHostsCheckError
from spicerack.remote import RemoteCheckError, RemoteExecutionError


__title__ = "Perform a rolling reboot of a conftool cluster"
logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def check_percentage(arg):
    """Type checker for a percentage between 0 and 100."""
    try:
        int_arg = int(arg)
    except ValueError:
        raise argparse.ArgumentTypeError("Percentage must be an integer.")
    if int_arg < 1 or int_arg > 100:
        raise argparse.ArgumentTypeError("Percentage must be between 1 and 100")
    return int_arg


def argument_parser():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('--datacenter', '-D', help='Datacenter where to restart the service', choices=CORE_DATACENTERS)
    parser.add_argument('--cluster', '-c', help='Cluster to restart')
    parser.add_argument('--percentage', '-p',
                        help='Percentage of the cluster to act upon at the same time',
                        type=check_percentage, default=5)
    parser.add_argument('--grace_sleep', '-s',
                        help='Pause taken between removing servers from the pool, and rebooting them. Defaults to 30 s',
                        default=30.0, type=float)
    parser.add_argument('--max_failed', '-m', help='Max Failed groups of execution', default=1, type=float)
    parser.add_argument('--no-fail-on-icinga', '-n',
                        help='Reboot is considered successful if icinga fails', action='store_true')
    parser.add_argument('--exclude', help='List of hosts that should not be rebooted, in NodeSet notation', default='')
    return parser


@retry(
    tries=25,
    delay=timedelta(seconds=3),
    backoff_mode='linear',
    exceptions=(IcingaError,),
)
def wait_for_icinga_optimal(icinga, remote_hosts):
    """Waits for an icinga optimal status, else raises an exception."""
    status = icinga.get_status(remote_hosts.hosts)
    if not status.optimal:
        failed = [
            '{}:{}'.format(k, ','.join(v)) for k, v in status.failed_services.items()
        ]
        raise IcingaError('Not all services are recovered: {}'.format(' '.join(failed)))


@attr.s
class Results:
    """Class used to manage result reporting."""

    hosts: List[str] = attr.ib()
    successful: List[str] = attr.ib()
    failed: List[str] = attr.ib()
    failed_slices: int = attr.ib(default=0)

    def fail(self, nodes: NodeSet):
        """Add nodes to the failed list."""
        self.failed.extend(nodes.striter())
        self.failed_slices += 1

    def success(self, nodes: NodeSet):
        """Add nodes to the success list."""
        self.successful.extend(nodes.striter())

    def report(self):
        """Report on results."""
        if self.failed_slices == 0:
            logger.info('All reboots were successful')
            return 0
        else:
            logger.info(
                'Reboots where successful for: {}'.format(','.join(self.successful))
            )
            logger.info('Groups with failed reboots: {}'.format(self.failed_slices))
            logger.info('Hosts in those groups: {}'.format(','.join(self.failed)))
            logger.info('Check the logs for specific failures')
            leftovers = list(set(self.hosts) - set(self.successful) - set(self.failed))
            if leftovers:
                logger.info(
                    'No action was performed for {}'.format(','.join(leftovers))
                )
            return 1


def reboot_with_downtime(spicerack, remote_hosts, results, no_fail_on_icinga):
    """Reboots a group of hosts, setting downtime."""
    icinga = spicerack.icinga()
    puppet = spicerack.puppet(remote_hosts)
    reason = spicerack.admin_reason('Rebooting hosts {}'.format(remote_hosts))
    try:
        with icinga.hosts_downtimed(
            remote_hosts.hosts, reason, duration=timedelta(minutes=20)
        ):
            reboot_time = datetime.utcnow()
            remote_hosts.reboot(batch_size=len(remote_hosts))
            remote_hosts.wait_reboot_since(reboot_time)
            puppet.wait_since(reboot_time)
            # First let's try to check if icinga is already in optimal state.
            # If not, we require a recheck all service, then
            # wait a grace period before declaring defeat.
            if not icinga.get_status(remote_hosts.hosts).optimal:
                icinga.recheck_all_services(remote_hosts.hosts)
                wait_for_icinga_optimal(icinga, remote_hosts)
        results.success(remote_hosts.hosts)
    except IcingaError as e:
        # Icinga didn't run correctly. log an error
        # but the servers will still be repooled,
        # unless it's explicitly disabled on the cli.
        results.fail(remote_hosts.hosts)
        if no_fail_on_icinga:
            logger.warning(e)
            results.success(remote_hosts.hosts)
        else:
            results.fail(remote_hosts.hosts)
            logger.error(e)
            logger.error('Hosts {} have NOT been repooled.'.format(','.join(results.hosts)))
            raise
    except (PuppetHostsCheckError, RemoteCheckError, RemoteExecutionError) as e:
        # Some host failed to come up again, or something fundamental broke.
        # log an error, exit *without* repooling
        logger.error(e)
        logger.error('Hosts {} have NOT been repooled.'.format(','.join(results.hosts)))
        results.fail(remote_hosts.hosts)
        raise


def run(args, spicerack):
    """Reboot the cluster"""
    confctl = spicerack.confctl('node')
    hosts_list = list(
        {obj.name for obj in confctl.get(dc=args.datacenter, cluster=args.cluster)}
    )
    remote_hosts = spicerack.remote().query(','.join(hosts_list))
    results = Results(hosts=remote_hosts, successful=[], failed=[])
    to_exclude = NodeSet(args.exclude)

    n_slices = math.ceil(1.0 / (args.percentage * 0.01))
    for raw_slice in remote_hosts.split(n_slices):
        if results.failed_slices > args.max_failed:
            logger.error('Too many failures, exiting')
            return results.report()
        hosts = raw_slice.hosts - to_exclude
        # If no host in the current slice, just move on to the next.
        if not hosts:
            continue
        # Let's avoid a second query to puppetdb here.
        remote_slice = spicerack.remote().query('D{{{h}}}'.format(h=str(hosts)))
        logger.info("Now acting on {}".format(str(hosts)))
        try:
            # We select only on the hostnames, as we're rebooting, so we just need
            # to depool the host pretty much everywhere.
            with confctl.change_and_revert(
                'pooled',
                'yes',
                'no',
                name='|'.join(remote_slice.hosts.striter()),
            ):
                time.sleep(args.grace_sleep)
                reboot_with_downtime(spicerack, remote_slice, results, args.no_fail_on_icinga)
        except Exception as e:
            # If an exception was raised within the context manager, we have some hosts
            # left depooled, so we stop the loop for human inspection.
            results.fail(remote_slice.hosts)
            logger.error('Unrecoverable error. Stopping the rolling reboot: {}'.format(e))
            break
    return results.report()
