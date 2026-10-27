"""etcd roll operations cookbook."""

import logging

from argparse import Namespace
from datetime import timedelta
from spicerack import Spicerack
from spicerack.remote import RemoteExecutionError, RemoteHosts
from spicerack.decorators import retry
from spicerack.remote import RemoteError
from cookbooks.sre import SREBatchBase, SREBatchRunnerBase
from cookbooks.sre.k8s import etcdctl, ALLOWED_CUMIN_ALIASES

ALLOWED_ALIASES = [k8s_data["etcd"] for k8s_data in ALLOWED_CUMIN_ALIASES.values()]


logger = logging.getLogger(__name__)


class EtcdRestartReboot(SREBatchBase):
    """Cookbook to perform a rolling reboot/restart of etcd

    Usage example:
        cookbook sre.k8s.roll-restart-reboot-etcd \
           --reason "Rolling reboot to pick up new kernel" reboot

        cookbook sre.k8s.roll-restart-reboot-etcd \
        --reason "Rolling restart to pick new OpenSSL" restart_daemons

    """

    batch_default = 1
    grace_sleep = 30

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return EtcdRestartRebootRunner(args, self.spicerack)


class EtcdRestartRebootRunner(SREBatchRunnerBase):
    """Roll reboot/restart an etcd cluster"""

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Fail at init if attempting to rolling reboot stacked master nodes"""
        try:
            super().__init__(args, spicerack)
        except RemoteError as exc:
            logger.error(
                "Never rolling reboot stacked control planes with this cookbook. Use sre.k8s.reboot-nodes instead."
            )
            raise RemoteError from exc

    @property
    def allowed_aliases(self):
        """Required by SREBatchRunnerBase"""
        return ALLOWED_ALIASES

    def _query(self) -> str:
        """Return the formatted query"""
        query = super()._query()
        if self._args.action == "reboot":
            query += " and not P{P:kubernetes::master}"
        return query

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
        return [etcdctl("endpoint health")]
