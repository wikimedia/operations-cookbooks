"""Hadoop Clusters Operations

There are three types of daemons running on the Hadoop workers:
- Yarn Nodemanager (on all hosts)
- HDFS Datanode (on all hosts)
- HDFS Journalnode (on a few hosts only)

The Yarn Nodemanager is a resource (CPU/Memory) manager for the running host:
it is responsible to spawn and control other jvms called "containers" (basically
map/reduce workers in Hadoop terminology). The Nodemanager can seamlessly
restart without impacting/killing the running jvm containers. The caveat is
that a container will keep using a old version of the jvm until its computation
finishes.

The HDFS Datanode is more delicate since it represents the daemon that controls
HDFS file system blocks for the node it runs on. All the datanodes collectively
represent the HDFS distributed file system. Restarting the daemons should be done
carefully and with slow pace since it might impact running jobs (making them fail)
and/or create corrupted/under-replicated blocks.

The HDFS Journalnode is a daemon deployed only on a few hosts, to support a distributed
edit stream for the HDFS file system (supporting HA between master nodes). The journalnodes
cluster can sustain up to n/2 failures without causing errors, but more than that
causes the HDFS Namenodes to shutdown (as precautionary measure).

"""
__title__ = "Hadoop Clusters Operations"

# List of Hadoop cluster names (reused across cookbooks)
HADOOP_CLUSTER_NAMES = ('test', 'analytics')

CLUSTER_CUMIN_ALIAS = 'A:hadoop'
MASTER_CUMIN_ALIAS = 'A:hadoop-master'
STANDBY_CUMIN_ALIAS = 'A:hadoop-standby'
WORKERS_CUMIN_ALIAS = 'A:hadoop-worker'
HDFS_JOURNAL_CUMIN_ALIAS = 'A:hadoop-hdfs-journal'
