"""Reboot all Kafka nodes in a cluster."""

import logging

from datetime import datetime, timedelta
from time import sleep

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from cookbooks.sre.kafka import parse_kafka_arguments


logger = logging.getLogger(__name__)


class RebootKafkaWorkers(CookbookBase):
    """Reboot all Kafka nodes in a cluster.

    The cookbook executes the following for each host in the cluster:
      1) Stop the kafka-mirror and kafka processes
      2) Reboot the node
      3) Wait 900s to make sure that any unbalanced/under-replicated/etc.. partition recovers.
      4) Force a prefered-replica-election to make sure that partition leaders are balanced
         before the next broker is restarted. This is not strictly needed since they should
         auto-rebalance, but there are rare use cases in which it might not happen.
      5) Sleep for args.batch_sleep_seconds before the next reboot
    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        return parse_kafka_arguments(description=self.__doc__,
                                     # logging-eqiad and logging-codfw are running elasticsearch
                                     # as well as Kafka, so to reboot them safely we'd have to account
                                     # for that.
                                     cluster_choices=['main-eqiad', 'jumbo', 'main-codfw', 'test-eqiad'])

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RebootKafkaWorkersRunner(args, self.spicerack)


class RebootKafkaWorkersRunner(CookbookRunnerBase):
    """Reboot kafka cluster cookbook runner."""

    def __init__(self, args, spicerack):
        """Reboot kafka on a given cluster."""
        ensure_shell_is_durable()

        self.icinga = spicerack.icinga()
        self.reason = spicerack.admin_reason('Reboot kafka nodes')
        self.puppet = spicerack.puppet
        self.remote = spicerack.remote()

        self.sleep_before_pref_replica_election = args.sleep_before_pref_replica_election
        self.batch_sleep_seconds = args.batch_sleep_seconds
        self.cluster = args.cluster

        cluster_cumin_alias = "A:kafka-" + args.cluster

        self.kafka_brokers = self.remote.query(cluster_cumin_alias)

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for Kafka {} cluster: {}'.format(self.cluster, self.reason)

    def reboot_kafka_node(self, host):
        """Reboot a single Kafka node."""
        node = self.remote.query('D{' + host + '}')
        puppet = self.puppet(node)

        with self.icinga.hosts_downtimed(host, self.reason, duration=timedelta(minutes=30)):
            with puppet.disabled(self.reason):
                logger.info('Stopping kafka processes on host %s', host)

                node.run_sync('systemctl stop kafka-mirror')
                node.run_sync('systemctl stop kafka')

                reboot_time = datetime.utcnow()
                node.reboot()
                node.wait_reboot_since(reboot_time)

                logger.info(
                    'Reboot completed for node %s. Waiting %s before running preferred-replica-election '
                    'for the broker to recover.',
                    self.sleep_before_pref_replica_election,
                    host
                )

                sleep(self.sleep_before_pref_replica_election)

                node.run_sync('source /etc/profile.d/kafka.sh; kafka preferred-replica-election')

    def run(self):
        """Reboot all Kafka nodes on a given cluster"""
        ask_confirmation(
            'Please check the Grafana dashboard of the cluster and make sure that '
            'topic partition leaders are well balanced and that all brokers are up and running.')

        logger.info('Checking that all Kafka brokers are reported up by their systemd unit status.')
        self.kafka_brokers.run_sync('systemctl status kafka')

        logger.info('Checking if /etc/profile.d/kafka.sh can be sourced.')
        self.kafka_brokers.run_sync('source /etc/profile.d/kafka.sh')

        if self.sleep_before_pref_replica_election < 900:
            ask_confirmation(
                'The sleep time between a node restart and kafka preferred-replica-election '
                'is less than 900 seconds. The broker needs some time to recover after a restart. '
                'Are you sure?')

        for host in self.kafka_brokers.hosts:
            logger.info('Starting reboot of kafka host %s', host)
            self.reboot_kafka_node(host)

            logger.info('Sleeping %s before next host', self.batch_sleep_seconds)
            sleep(self.batch_sleep_seconds)

        logger.info('All Kafka node reboots completed!')
