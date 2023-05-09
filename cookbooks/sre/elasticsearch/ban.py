"""Ban nodes based on hostname or other attributes."""

import argparse
import logging
import yaml

from elasticsearch import Elasticsearch
from spicerack.cookbook import CookbookBase, CookbookRunnerBase

from wmflib.constants import CORE_DATACENTERS
from cookbooks.sre.elasticsearch import CLUSTERGROUPS


__title__ = __doc__
logger = logging.getLogger(__name__)


class BanNode(CookbookBase):
    """Ban nodes based on host selection string."""

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(prog=__name__, description=__title__,
                                         formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument('action', choices=('ban', 'unban'),
                            help='One of: %(choices)s.')
        parser.add_argument('clustergroup', choices=CLUSTERGROUPS, help='Name of clustergroup. One of: %(choices)s.')
        parser.add_argument('admin_reason', help='Administrative Reason')
        parser.add_argument('--task-id', help='task_id for the change')
        parser.add_argument('--hosts', help='host(s) to ban from all clusters')
        parser.add_argument('--row', help='row to ban from all clusters')

        return parser

    def get_runner(self, args):
        """Orchestrates cluster operations"""
        clustergroup = args.clustergroup
        elasticsearch_clusters = self.spicerack.elasticsearch_clusters(args.clustergroup, CORE_DATACENTERS)
        reason = self.spicerack.admin_reason(args.admin_reason, task_id=args.task_id)
        # TODO: Validate host selection
        return BanNodeRunner(self.spicerack, elasticsearch_clusters, clustergroup,
                             args.action, args.hosts, args.row, reason)


class BanNodeRunner(CookbookRunnerBase):
    """Ban Elastic nodes from cluster using cluster settings API."""

    # pylint: disable=too-many-arguments
    def __init__(self, spicerack, elasticsearch_clusters, clustergroup, action, hosts, row, reason):
        """Initialize the BanNode Runner."""
        self.spicerack = spicerack
        self.elasticsearch_clusters = elasticsearch_clusters
        self.clustergroup = clustergroup
        self.action = action
        self.hosts = hosts
        self.reason = reason
        self.row = row
        # The following file is found on all cumin hosts, seems the simplest way to
        # dynamically load the cluster endpoints
        with open('/etc/spicerack/elasticsearch/config.yaml', 'r', encoding='utf8') as file:
            self.es_cluster_info = yaml.safe_load(file)

    @property
    def runtime_description(self):
        """Return a string that represents hosts to ban."""
        if self.hosts:
            return "Banning hosts: {} for {}".format(self.hosts, self.reason)
        if self.row:
            return "Banning all hosts in row {} for {}".format(self.row, self.reason)
        if self.action == "unban":
            return "Unbanning all hosts in {}".format(self.clustergroup)
        raise ValueError

    def run(self):
        """Ban hosts based on host selection"""
        if self.action == "unban":
            if self.hosts or self.row:
                raise ValueError("Bad argument: action={} takes no arguments".format(self.action))
            for cluster_name in self.es_cluster_info["search"][self.clustergroup]:
                self.ban_or_unban_nodes(None, cluster_name)
        else:
            if not self.hosts and not self.row:
                raise ValueError("Bad argument: action={}, but neither host nor row were provided.".format(self.action))
            if self.hosts:
                elastic_hosts = self.spicerack.remote().query(self.hosts)
            elif self.row:
                # TODO: Implement row logic
                pass
            target_hosts = [host.split('.')[0] for host in elastic_hosts.hosts]
            cluster_names = self.es_cluster_info["search"][self.clustergroup]
            # transform the host IDs slightly, to the form known by elasticsearch.
            # not all hosts belong to all clusters, but at the moment we are OK with banning
            # non-existent nodes, as it does not harm the clusters.
            for cluster_name in cluster_names:
                if "cloudelastic" in cluster_name:
                    ce_cluster_name = f"{cluster_name.split('-https')[0]}-eqiad"
                    instance_names = [f"{host}-{ce_cluster_name}" for host in target_hosts]
                else:
                    instance_names = [f"{host}-{cluster_name}" for host in target_hosts]
                self.ban_or_unban_nodes(instance_names, cluster_name)

    def ban_or_unban_nodes(self, node_names, cluster_name):
        """Ban or unban nodes using Elasticsearch Cluster API."""
        es_endpoint = self.es_cluster_info["search"][self.clustergroup][cluster_name]
        es_client = Elasticsearch([es_endpoint],
                                  use_ssl=True,
                                  timeout=30)
        cluster_settings = es_client.cluster.get_settings()
        for duration in ["persistent", "transient"]:
            logger.info("current %s cluster settings: %s", duration, cluster_settings[duration])
        if self.action == "unban":
            logger.info("Unbanning all nodes using endpoint %s[]", es_endpoint)
            unban_dict = {"persistent": {"cluster.routing.allocation.exclude": {"_host": "", "_ip": "", "_name": ""}}}
            unban_action = es_client.cluster.put_settings(unban_dict)  # type: ignore [misc]
            logger.info(unban_action)
        if self.action == "ban":
            hosts_to_ban = ",".join(node_names)
            logger.info("Preparing to ban %s using endpoint %s", hosts_to_ban, es_endpoint)
            ban_dict = {"persistent": {"cluster.routing.allocation.exclude._name": hosts_to_ban}}
            ban_action = es_client.cluster.put_settings(ban_dict)  # type: ignore [misc]
            logger.info(ban_action)
