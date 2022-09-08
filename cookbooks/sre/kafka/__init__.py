"""Kafka Clusters Operations

Kafka brokers can be restarted / rebooted one at the time, usually
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
"""

import argparse

from spicerack.cookbook import ArgparseFormatter


__title__ = __doc__


def parse_kafka_arguments(description, cluster_choices):
    """Helper for arguments that are shared between Kafka cookbooks."""
    combined_description = '\n\n'.join([__doc__, 'Cookbook-specific documentation:', description])
    parser = argparse.ArgumentParser(description=combined_description, formatter_class=ArgparseFormatter)
    parser.add_argument('cluster', help='The name of the Kafka cluster to work on.',
                        choices=cluster_choices)
    parser.add_argument('--batch-sleep-seconds', type=float, default=300.0,
                        help="Seconds to sleep between each broker restart.")
    parser.add_argument('--sleep-before-pref-replica-election', type=int, default=900,
                        help="Seconds to sleep between a broker restart and "
                             "the kafka preferred-replica-election execution.")
    return parser
