"""Restart all Kafka broker daemons in a cluster."""
import logging
from argparse import Namespace
from datetime import timedelta

from spicerack import Spicerack
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.interactive import ask_confirmation

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase

logger = logging.getLogger(__name__)

CLUSTER_CHOICES = (
    "main-eqiad",
    "main-codfw",
    "jumbo-eqiad",
    "logging-eqiad",
    "logging-codfw",
    "test-eqiad",
)


class RollRestartRebootBrokers(SREBatchBase):
    """Restart or reboot all Kafka brokers on a given cluster.

    The cookbook executes the following for each Kafka broker host in the cluster:
    1) Make sure the kafka broker service is running and in sync
    2) Restart the kafka broker processes / reboot the host
    3) Wait until any unbalanced/under-replicated/etc.. partition has recovered.
    4) Force a prefered-replica-election to make sure that partition leaders are balanced
        before the next broker is restarted. This is not strictly needed since they should
        auto-rebalance, but there are rare use cases in which it might not happen.
    5) Sleep for args.grace_sleep before the next kafka broker restart

    Usage example:
        cookbook sre.kafka.roll-restart-brokers \
            --alias kafka-jumbo-eqiad \
            --reason "reload broker config" \
            restart_daemons
        cookbook sre.kafka.roll-restart-brokers \
            --alias kafka-main-eqiad \
            --reason "upgrade kernel" \
            reboot

    """

    batch_max = 1  # Only restart one broker at a time
    grace_sleep = 300  # By default, wait 5 min between brokers
    min_grace_sleep = 120  # Don't allow going under 2 minutes between 2 broker restarts

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollingActionBrokersRunner(args, self.spicerack)


class RollingActionBrokersRunner(SREBatchRunnerBase):
    """Kafka brokers roll restart/reboot runner class"""

    def __init__(self, args: Namespace, spicerack: Spicerack):
        """Initialize the runner."""
        super().__init__(args, spicerack)
        ask_confirmation(
            "Please check the Grafana dashboard of the cluster "
            "(https://grafana.wikimedia.org/d/000000027/kafka"
            f"?&var-kafka_cluster={args.alias.replace('kafka-', '')}) "
            "and make sure that topic partition leaders are well balanced and that all brokers "
            "are up and running."
        )

    @property
    def allowed_aliases(self) -> list[str]:
        """Required by SREBatchRunnerBase"""
        return [f"kafka-{alias}" for alias in CLUSTER_CHOICES]

    @property
    def allowed_aliases_query(self) -> str:
        """Required by SREBatchRunnerBase"""
        return "A:kafka-all"

    @property
    def restart_daemons(self) -> list[str]:
        """Property to return a list of daemons to restart"""
        return ["kafka.service"]

    @retry(
        tries=30,
        delay=timedelta(seconds=30),
        backoff_mode="constant",
        exceptions=(
            # raised by RemoteHosts.run_sync if the script exits with a status != 0
            RemoteExecutionError,
        ),
    )
    def _run_scripts(self, scripts: list[str], hosts: RemoteHosts) -> None:
        """Retry running the argument scrips until the exit with status 0 or timeout"""
        for script in scripts:
            hosts.run_sync(script)

    @property
    def pre_scripts(self) -> list:
        """Make sure the broker is fully in-sync before executing the action"""
        return [
            "systemctl is-active --quiet kafka.service",
            # We make sure that sourcing kafka.sh works, as it will be used
            # in the post action, *after* the broker restart. If it somehow
            # fails at that point, the leader election step will fail.
            "source /etc/profile.d/kafka.sh",
            # exits with status 1 if the current broker isn't in sync
            "/usr/local/bin/kafka-broker-in-sync",
        ]

    @property
    def post_scripts(self) -> list:
        """Retry until the broker is fully back in sync"""
        return [
            "systemctl is-active --quiet kafka.service",
            # exits with status 1 if the current broker isn't in sync
            "/usr/local/bin/kafka-broker-in-sync",
            "source /etc/profile.d/kafka.sh; kafka preferred-replica-election",
        ]
