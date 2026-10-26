"""etcd roll operations cookbook."""

from argparse import ArgumentParser
from datetime import timedelta
from itertools import product
from spicerack.remote import RemoteExecutionError, RemoteHosts
from spicerack.decorators import retry
from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

K8S_CLUSTERS = ("aux", "wikikube", "wikikube-staging", "ml-serve", "dse-k8s")
SITES = ("codfw", "eqiad")
ALLOWED_ALIASES = [f"{cluster}-etcd-{site}" for cluster, site in product(K8S_CLUSTERS, SITES)] + ["ml-staging-etcd"]


class EtcdRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of etcd

    Usage example:
        cookbook sre.etcd.roll-restart-reboot \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.etcd.roll-restart-reboot \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()
        for action in parser._actions:
            if action.dest == "alias":
                action.choices = ALLOWED_ALIASES
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return EtcdRestartRebootRunner(args, self.spicerack)


class EtcdRestartRebootRunner(SREBatchRunnerBase):
    """Roll reboot/restart an etcd cluster"""

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ALLOWED_ALIASES

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ["etcd"]

    @retry(
        tries=60,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        exceptions=(RemoteExecutionError,),
    )
    def post_action(self, hosts: RemoteHosts) -> None:
        return super().post_action(hosts=hosts)

    @property
    def post_scripts(self) -> list:
        """Ensure that etcd is running and healthy"""
        return ["etcdctl endpoint health --endpoints=https://$(hostname -f):2379"]
