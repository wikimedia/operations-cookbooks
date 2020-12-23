"""Restart all Kafka broker daemons in a cluster

Kafka brokers can be restarted one at the time, usually
without any pre-step (like traffic draining, etc..).

As refresh:
* Kafka manages topics, and every topic can
  be split into multiple partitions. Every partition is
  then replicated across multiple brokers, in our case
  three times.
* Every client can decide how soon it wants the broker to
  ACK that it has received a message when producing it
  to a certain topic/partition.
  For example, it could be requested that only one broker
  acknowledges the message, or two (so one replica confirms
  to have received the message).
* The message will then be replicated three times (this is our
  default setting) on multiple brokers.
* Every broker can act as Leader for a given topic partition.
  This means that producers will be directed to it when producing
  messages for that topic partition.

There are some things to keep into consideration:
1) Before restarting a Kafka broker, it is better to make sure
   that partition leadership assigments are split evenly across
   the brokers. This is easily doable checking metrics in Grafana.
   It is not a strict requirement but if the cluster is already
   unbalanced and one broker is stopped/restarted, then it might
   become even more unbalanced and producers might suffer from it.
   In more recent versions of Kafka the following command is available:
     kafka topics --describe --under-replicated-partitions
     https://cwiki.apache.org/confluence/display/KAFKA/Replication+tools
2) It is really better to avoid more than one Broker down at the same
   time. We can sustain two brokers down without data loss (caveat:
   see what it is written above about consistency and producers) but
   it is better to restart one broker at the time to avoid risking
   availability (if a random crash of another broker happens at the
   same time it becomes a big problem).

This cookbook it is really simple, but it should be a good first step
in automating Kafka restarts. The cookbook executes the following for
each host in the cluster:
  1) Restart the kafka broker.
  2) Wait 900s to make sure that any unbalanced/under-replicated/etc.. partition recovers.
  3) Force a prefered-replica-election to make sure that partition leaders are balanced
     before the next broker is restarted. This is not strictly needed since they should
     auto-rebalance, but there are rare use cases in which it might not happen.
  4) Sleep for args.batch_sleep_seconds before the next restart

"""
import argparse
import logging

from datetime import timedelta

from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = 'Roll restart all the Kafka brokers on a cluster'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Kafka cluster to work on.',
                        choices=['main-eqiad', 'jumbo', 'main-codfw',
                                 'logging-eqiad', 'logging-codfw'])
    parser.add_argument('--batch-sleep-seconds', type=float, default=300.0,
                        help="Seconds to sleep between each broker restart.")
    parser.add_argument('--sleep-before-pref-replica-election', type=int, default=900,
                        help="Seconds to sleep between a broker restart and "
                             "the kafka preferred-replica-election execution.")
    return parser


def run(args, spicerack):
    """Restart all Kafka brokers on a given cluster"""
    cluster_cumin_alias = "A:kafka-" + args.cluster

    ensure_shell_is_durable()

    """Required by Spicerack API."""
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
