r"""WMCS Toolforge - Depool and delete the given k8s worker node from a toolforge installation

Usage example:
    cookbook wmcs.toolforge.remove_k8s_worker_node \
        --project toolsbeta \
        --node-fqdn toolsbeta-test-k8s-worker-4.toolsbeta.eqiad1.wikimedia.cloud \
        --k8s-worker-prefix toolsbeta-test-k8s-worker

"""
import logging

from cookbooks.wmcs.toolforge.worker.depool_and_remove_node import ToolforgeDepoolAndRemoveNode

LOGGER = logging.getLogger(__name__)


class ToolforgeRemoveK8sWorkerNode(ToolforgeDepoolAndRemoveNode):
    """WMCS Toolforge cookbook to remove and delete an existing k8s worker node"""

    title = __doc__
