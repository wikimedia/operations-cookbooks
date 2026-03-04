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
   We also run /usr/local/bin/kafka-broker-in-sync on each Kafka broker.
2) It is really better to avoid more than one Broker down at the same
   time. We can sustain two brokers down without data loss (caveat:
   see what it is written above about consistency and producers) but
   it is better to restart one broker at the time to avoid risking
   availability (if a random crash of another broker happens at the
   same time it becomes a big problem).
3) Better to separate the work between brokers acting as controller
   and brokers not acting as controllers to avoid issues.
   See https://phabricator.wikimedia.org/T399005
"""

from spicerack.remote import RemoteHosts
from spicerack import Spicerack

__owner_team__ = "Data Platform"

CLUSTER_CHOICES = (
    "main-eqiad",
    "main-codfw",
    "jumbo-eqiad",
    "logging-eqiad",
    "logging-codfw",
    "test-eqiad",
)


def get_preferred_replica_election_command(broker_remote: RemoteHosts) -> str:
    """Return the Kafka preferred replica command based on the Kafka version."""
    # In recent Kafka versions there are some scripts/API to contact, but to keep
    # compatibility with 1.1 we need to be creative and look into Confluent's jar versions.
    res = broker_remote.run_sync("ls /usr/share/java/kafka/kafka_*.jar 2>/dev/null "
                                 "| grep -oE '[0-9]+\\.[0-9]+\\.[0-9]+' | head -1")
    for _, output in res:
        kafka_version = output.message().decode()
    if kafka_version.startswith("1.1"):
        return "kafka preferred-replica-election"
    return "kafka leader-election --election-type PREFERRED --all-topic-partitions"


def get_cluster_controller_host(spicerack: Spicerack, kafka_cluster_name: str) -> str:
    """Return the hostname of the controller of the kafka cluster being acted upon."""
    if kafka_cluster_name not in CLUSTER_CHOICES:
        raise RuntimeError(
              f"The kafka cluster {kafka_cluster_name} is not among "
              f"the allowed ones: {CLUSTER_CHOICES}")
    cluster_name, site = kafka_cluster_name.split("-")
    admin_client = spicerack.kafka().admin_client(site=site, cluster_name=cluster_name)
    cluster_state = admin_client.describe_cluster()
    for broker_details in cluster_state["brokers"]:
        if broker_details["node_id"] == cluster_state["controller_id"]:
            return broker_details["host"]
    raise RuntimeError(
        f"No registered broker matched the kafka cluster controller {cluster_state['controller_id']}"
    )
