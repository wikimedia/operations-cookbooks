"""Restart all Kafka broker daemons in a cluster."""
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.kafka import parse_kafka_arguments

logger = logging.getLogger(__name__)


class RollRestartBrokers(CookbookBase):
    """Restart all Kafka brokers on a given cluster.

    The cookbook executes the following for each Kafka broker host in the cluster:
    1) Restart the kafka broker processes
    2) Wait 900s to make sure that any unbalanced/under-replicated/etc.. partition recovers.
    3) Force a prefered-replica-election to make sure that partition leaders are balanced
        before the next broker is restarted. This is not strictly needed since they should
        auto-rebalance, but there are rare use cases in which it might not happen.
    4) Sleep for args.batch_sleep_seconds before the next kafka broker restart

    Usage example:
        cookbook sre.kafka.roll-restart-brokers jumbo-eqiad
        cookbook sre.kafka.roll-restart-brokers --batch-sleep-seconds 600 main-eqiad
        cookbook sre.kafka.roll-restart-brokers --sleep-before-pref-replica-election 1800 logging-codfw

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        return parse_kafka_arguments(description=self.__doc__,
                                     cluster_choices=['main-eqiad', 'main-codfw', 'jumbo-eqiad',
                                                      'logging-eqiad', 'logging-codfw', 'test-eqiad'])

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartBrokersRunner(args, self.spicerack)


class RollRestartBrokersRunner(CookbookRunnerBase):
    """Kafka brokers roll restart runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self.cluster_cumin_alias = "A:kafka-" + args.cluster
        self.kafka_brokers = spicerack.remote().query(self.cluster_cumin_alias)
        self.icinga_hosts = spicerack.icinga_hosts(self.kafka_brokers.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of jvm daemons for openjdk upgrade.')
        self.batch_sleep_seconds = args.batch_sleep_seconds
        self.sleep_before_pref_replica_election = args.sleep_before_pref_replica_election

        ask_confirmation(
            'Please check the Grafana dashboard of the cluster and make sure that '
            'topic partition leaders are well balanced and that all brokers are up and running.')

        if args.sleep_before_pref_replica_election < 900:
            ask_confirmation(
                'The sleep time between a broker restart and kafka preferred-replica-election '
                'is less than 900 seconds. The broker needs some time to recover after a restart. '
                'Are you sure?')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Kafka {} cluster: {}'.format(self.cluster_cumin_alias, self.admin_reason.reason)

    def run(self):
        """Restart all Kafka brokers on a given cluster"""
        logger.info('Checking that all Kafka brokers are reported up by their systemd unit status.')
        self.kafka_brokers.run_sync('systemctl status kafka')

        logger.info('Checking if /etc/profile.d/kafka.sh can be sourced.')
        self.kafka_brokers.run_sync('source /etc/profile.d/kafka.sh')

        with self.icinga_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=240)):
            commands = [
                'systemctl restart kafka',
                'sleep ' + str(self.sleep_before_pref_replica_election),
                'source /etc/profile.d/kafka.sh; kafka preferred-replica-election',
            ]
            self.kafka_brokers.run_async(*commands, batch_size=1, batch_sleep=self.batch_sleep_seconds)

    logger.info('All Kafka broker restarts completed!')
