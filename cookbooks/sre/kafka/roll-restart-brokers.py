"""Restart all Kafka broker daemons in a cluster.

The cookbook executes the following for each Kafka broker host in the cluster:
  1) Restart the kafka broker processes
  2) Wait 900s to make sure that any unbalanced/under-replicated/etc.. partition recovers.
  3) Force a prefered-replica-election to make sure that partition leaders are balanced
     before the next broker is restarted. This is not strictly needed since they should
     auto-rebalance, but there are rare use cases in which it might not happen.
  4) Sleep for args.batch_sleep_seconds before the next kafka broker restart
"""
import logging

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks.sre.kafka import parse_kafka_arguments


__title__ = 'Roll restart all the Kafka brokers on a cluster'
logger = logging.getLogger(__name__)


# TODO refactor this module to the class api
def argument_parser():
    """As specified by Spicerack API."""
    return parse_kafka_arguments(description=__doc__,
                                 cluster_choices=['main-eqiad', 'jumbo', 'main-codfw',
                                                  'logging-eqiad', 'logging-codfw', 'test'])


def run(args, spicerack):
    """Restart all Kafka brokers on a given cluster"""
    cluster_cumin_alias = "A:kafka-" + args.cluster

    ensure_shell_is_durable()

    kafka_brokers = spicerack.remote().query(cluster_cumin_alias)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of jvm daemons for openjdk upgrade.')

    ask_confirmation(
        'Please check the Grafana dashboard of the cluster and make sure that '
        'topic partition leaders are well balanced and that all brokers are up and running.')

    logger.info('Checking that all Kafka brokers are reported up by their systemd unit status.')
    kafka_brokers.run_sync('systemctl status kafka')

    logger.info('Checking if /etc/profile.d/kafka.sh can be sourced.')
    kafka_brokers.run_sync('source /etc/profile.d/kafka.sh')

    if args.sleep_before_pref_replica_election < 900:
        ask_confirmation(
            'The sleep time between a broker restart and kafka preferred-replica-election '
            'is less than 900 seconds. The broker needs some time to recover after a restart. '
            'Are you sure?')

    with icinga.hosts_downtimed(kafka_brokers.hosts, reason,
                                duration=timedelta(minutes=240)):
        commands = [
            'systemctl restart kafka',
            'sleep ' + str(args.sleep_before_pref_replica_election),
            'source /etc/profile.d/kafka.sh; kafka preferred-replica-election',
        ]
        kafka_brokers.run_async(*commands, batch_size=1, batch_sleep=args.batch_sleep_seconds)

    logger.info('All Kafka broker restarts completed!')
