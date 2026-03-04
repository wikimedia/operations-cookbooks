"""Move one or more Kafka brokers to a different Confluent distribution version."""
import logging
import time

from datetime import timedelta
from functools import cached_property

from cumin import nodeset
from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, confirm_on_failure

from . import CLUSTER_CHOICES, get_cluster_controller_host, get_preferred_replica_election_command

logger = logging.getLogger(__name__)


class ChangeConfluentDistributionVersion(CookbookBase):
    """Change Confluent distribution version.

    The cookbook changes the Confluent distribution version on a given
    Kafka cluster.
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('cluster', help='The name of the Kafka cluster to work on.',
                            choices=CLUSTER_CHOICES)
        parser.add_argument('--kafka-restart-sleep-seconds', type=float, default=300.0,
                            help="Seconds to sleep after starting the upgraded kafka service")
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return ChangeConfluentDistributionVersionRunner(args, self.spicerack)


class ChangeConfluentDistributionVersionRunner(CookbookRunnerBase):
    """Change Confluent distribution version runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        self.cluster_cumin_alias = "A:kafka-" + args.cluster
        self.brokers_hosts = spicerack.remote().query(
            self.cluster_cumin_alias).hosts
        self.alerting_hosts = spicerack.alerting_hosts(self.brokers_hosts)
        self.admin_reason = spicerack.admin_reason('Change Confluent distribution.')
        self.kafka_restart_sleep_seconds = args.kafka_restart_sleep_seconds
        self.spicerack = spicerack
        self.args = args

    @cached_property
    def cluster_controller_host(self) -> str:
        """Return the hostname of the controller of the kafka cluster being acted upon"""
        return get_cluster_controller_host(self.spicerack, self.args.cluster)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'Change Confluent distribution for Kafka {} cluster: {}'.format(
            self.cluster_cumin_alias, self.admin_reason.reason)

    def _upgrade_kafka_broker(self, broker_host: nodeset):
        remote_host = self.spicerack.remote().query(str(broker_host))

        logger.info("Stop kafka on %s", broker_host)
        remote_host.run_sync('/usr/bin/systemctl stop kafka')

        logger.info("Force puppet on %s to change the kafka package", broker_host)
        puppet_host = self.spicerack.puppet(remote_host)
        puppet_host.enable(self.admin_reason)
        puppet_host.run()

        # This is technically not needed since the package install
        # should start it, but better be safe than sorry.
        logger.info("Start kafka on %s", broker_host)
        remote_host.run_sync('/usr/bin/systemctl start kafka')

        logger.info(
            "Sleep %s seconds before checking the partition status.",
            self.kafka_restart_sleep_seconds)
        time.sleep(self.kafka_restart_sleep_seconds)

        logger.info("Check if brokers are in sync.")
        confirm_on_failure(
            remote_host.run_sync, "/usr/local/bin/kafka-broker-in-sync"
        )
        logger.info(
            "Note: not running preferred-replica-election since "
            "the command varies between Confluent Kafka distributions "
            "and it is not supported in rolling upgrades."
        )

    def _run_preferred_replica_election(self):
        remote_host = self.spicerack.remote().query(self.cluster_controller_host)
        command = get_preferred_replica_election_command(remote_host)
        remote_host.run_sync(f"source /etc/profile.d/kafka.sh; {command}")

    def run(self):
        """Change Confluent distribution version on a given cluster"""
        with self.alerting_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=120)):
            logger.info("Disable puppet on all brokers.")
            brokers_remote = self.spicerack.remote().query(str(self.brokers_hosts))
            brokers_puppet = self.spicerack.puppet(brokers_remote)
            brokers_puppet.disable(self.admin_reason)

            ask_confirmation(
                "Please merge the Confluent distribution change in Puppet. "
                "Continue only after puppet-merge has completed."
            )

            logger.info("Change Confluent distribution on all brokers except "
                        "the one acting as controller.")
            for broker_host in (self.brokers_hosts - nodeset(self.cluster_controller_host)):
                self._upgrade_kafka_broker(broker_host)

            logger.info("Changing Confluent distribution on the Kafka broker "
                        "acting as controller.")
            self._upgrade_kafka_broker(nodeset(self.cluster_controller_host))

        logger.info("All brokers have the new Confluent distribution.")

        logger.info("Run preferred-replica-election.")
        self._run_preferred_replica_election()
