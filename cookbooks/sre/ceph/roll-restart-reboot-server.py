"""Restart all Kafka broker daemons in a cluster."""

import logging
from argparse import ArgumentParser
from datetime import timedelta

from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)


CLUSTER_CHOICES = ("cephosd-codfw", "cephosd-eqiad")
DAEMON_CHOICES = ("mon", "mgr", "osd", "mds", "radosgw", "crash")


class RollRestartRebootCeph(SREBatchBase):
    """Rolling restart/reboot ceph servers/services

    This cookbook can be used to either:
    - rolling reboot part of / a whole ceph cluster
    - rolling restart all/some services in part of / a whole ceph cluster

    Examples:
    Usage example:
        # Restart all ceph services, one host after the other, in the whole cephosd cluster
        cookbook sre.ceph.roll-restart-reboot-servers \
            --alias cephosd-eqiad \
            --reason "Reload server config" \
            --task-id T12346 \
            restart_daemons

        # Restart the ceph-mgr service, one host after the other, in the whole cephosd cluster
        cookbook sre.ceph.roll-restart-reboot-servers \
            --alias cephosd-eqiad \
            --reason "Reload server config" \
            --task-id T12346 \
            --daemons mgr
            restart_daemons

        # Restart the mds and mon services on a single node
        cookbook sre.ceph.roll-restart-reboot-servers \
            --query 'P{cephosd1001.eqiad.wmnet}' \
            --reason "Restart services" \
            --task-id T12346 \
            --daemons mds,mon\
            restart_daemons

        # Reboot one host after the other, in the whole cephosd cluster
        cookbook sre.ceph.roll-restart-reboot-servers \
            --alias cephosd-eqiad \
            --reason "Kernel upgrade" \
            --task-id T12346 \
            reboot

    """

    batch_max = 1  # Only restart one broker at a time
    grace_sleep = 300  # By default, wait 5 min between brokers
    min_grace_sleep = 120  # Don't allow going under 2 minutes between 2 broker restarts

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        parser.add_argument(
            "--daemons",
            nargs="+",
            choices=DAEMON_CHOICES,
            help="The ceph services to restart",
            default=DAEMON_CHOICES,
        )
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollingActionBrokersRunner(args, self.spicerack)


class RollingActionBrokersRunner(SREBatchRunnerBase):
    """Ceph server roll restart/reboot runner class"""

    @property
    def allowed_aliases(self) -> list[str]:
        """Required by SREBatchRunnerBase"""
        return list(CLUSTER_CHOICES)

    @property
    def restart_daemons(self) -> list[str]:
        """Property to return a list of daemons to restart"""
        services = [
            f"ceph-{svc}.target" for svc in self._args.daemons if svc != "crash"
        ]
        if "crash" in self._args.daemons:
            services.append("ceph-crash.service")
        return services

    def pre_action(self, hosts: RemoteHosts) -> None:
        """Prevent Ceph from rebalancing the data while the host/service is down"""
        # Make sure the cluster is in a healthy state before we restart/reboot anything
        self.wait_for_cluster(hosts, "ceph health | grep HEALTH_OK")
        # Note: I have confirmed that the $(hostname) subcommand is executed on the target
        # host, and thus will be eg cephosd1001
        self.wait_for_cluster(hosts, "ceph osd set-group noout $(hostname)")
        super().pre_action(hosts)

    def post_action(self, hosts: RemoteHosts) -> None:
        """Re-enable automatic data rebalancing after the host/service is back up"""
        self.wait_for_cluster(hosts, "ceph osd unset-group noout $(hostname)")
        # Make sure the cluster is in a healthy state before we move on
        self.wait_for_cluster(hosts, "ceph health | grep HEALTH_OK")
        super().post_action(hosts)

    @retry(
        tries=30,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        exceptions=(
            # raised by RemoteHosts.run_sync if the script exits with a status != 0
            RemoteExecutionError,
        ),
    )
    def wait_for_cluster(self, hosts: RemoteHosts, command: str) -> None:
        """Retry running the argument scripts until the exit with status 0 or timeout"""
        hosts.run_sync(command)
