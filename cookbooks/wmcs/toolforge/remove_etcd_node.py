"""WMCS Toolforge - Depool and delete the given etcd node from a toolforge installation

Usage example:
    cookbook wmcs.toolforge.remove_etcd_node \
        --project toolsbeta \
        --node-fqdn toolsbeta-test-etcd-8.toolsbeta.eqiad1.wikimedia.cloud \
        --etcd-prefix toolsbeta-test-etcd

"""
import logging

from cookbooks.wmcs.toolforge.etcd.depool_and_remove_node import ToolforgeDepoolAndRemoveNode

LOGGER = logging.getLogger(__name__)


class ToolforgeRemoveEtcdNode(ToolforgeDepoolAndRemoveNode):
    """WMCS Toolforge cookbook to remove and delete an existing etcd node"""

    title = __doc__
