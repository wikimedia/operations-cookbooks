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
__owner_team__ = "Data Platform"

# List of Hadoop cluster names (reused across cookbooks)
HADOOP_CLUSTER_NAMES = ('test', 'analytics')

CLUSTER_CUMIN_ALIAS = 'A:hadoop'
MASTER_CUMIN_ALIAS = 'A:hadoop-master'
STANDBY_CUMIN_ALIAS = 'A:hadoop-standby'
WORKERS_CUMIN_ALIAS = 'A:hadoop-worker'
HDFS_JOURNAL_CUMIN_ALIAS = 'A:hadoop-hdfs-journal'
HADOOP_CLIENT_CUMIN_ALIASES = [
    'A:hadoop-dumps-client', 'A:an-airflow',
    'A:an-launcher', 'A:an-presto', 'A:druid-analytics',
    'A:druid-public', 'A:hadoop-client', 'A:hadoop-hue', 'A:hadoop-yarn',
    'A:hadoop-coordinator-primary', 'A:hadoop-coordinator-secondary']
HADOOP_TEST_CLIENT_CUMIN_ALIASES = [
    'A:druid-test', 'A:hadoop-client-test', 'A:hadoop-ui-test',
    'A:hadoop-coordinator-test', 'A:an-presto-test']
BIGTOP_WORKER_PACKAGES = [
    'bigtop-jsvc', 'bigtop-utils', 'hadoop', 'hadoop-client', 'hadoop-hdfs',
    'hadoop-hdfs-datanode', 'hadoop-hdfs-journalnode', 'hadoop-mapreduce',
    'hadoop-yarn', 'hadoop-yarn-nodemanager', 'hive', 'hive-hcatalog', 'hive-jdbc',
    'libhdfs0', 'sqoop', 'zookeeper']
BIGTOP_MASTER_PACKAGES = [
    'bigtop-jsvc', 'bigtop-utils', 'hadoop', 'hadoop-client', 'hadoop-hdfs',
    'hadoop-hdfs-namenode', 'hadoop-hdfs-zkfc', 'hadoop-mapreduce',
    'hadoop-mapreduce-historyserver', 'hadoop-yarn', 'hadoop-yarn-resourcemanager',
    'libhdfs0', 'zookeeper']
BIGTOP_MASTER_STANDBY_PACKAGES = [
    'bigtop-jsvc', 'bigtop-utils', 'hadoop', 'hadoop-client', 'hadoop-hdfs',
    'hadoop-hdfs-namenode', 'hadoop-hdfs-zkfc', 'hadoop-mapreduce',
    'hadoop-yarn', 'hadoop-yarn-resourcemanager',
    'libhdfs0', 'zookeeper']

# Some packages that are shipped by the CDH distribution are not available
# for BigTop, so the cookbook needs to workaround this filtering the list
# of packages to install.
CDH_PACKAGES_NOT_IN_BIGTOP = ('avro-libs', 'hadoop-0.20-mapreduce', 'kite',
                              'parquet', 'parquet-format', 'sentry')

# Due to the high number of Hadoop worker nodes, remote commands are tolerated
# to fail up to a 10% threshold to avoid unnecessary failures of cookbook.
HADOOP_WORKERS_CUMIN_SUCCESS_THRESHOLD = 0.9
