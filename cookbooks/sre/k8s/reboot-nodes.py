"""Reboot all nodes in a Kubernetes cluster.

The cookbook will split all nodes of a cluster into groups by taints and
then process each taint group in the following way:

For batches of nodes in the Kubernetes cluster it will:
- Cordon all hosts
- Drain all hosts
- Set their pooled status to inactive
- Set Icinga/Alertmanager downtime for all hosts in the batch to reboot
- Reboot
- Wait for hosts to come back online
- If reboot: Wait for the first puppet run
- Wait for Icinga optimal status
- Uncordon all hosts
- Set their pooled status to yes
- Remove the Icinga/Alertmanager downtime

After the first batch has been processed, it will try to avoid re-scheduling of
Pod's as far as possible by cordoning all hosts that are still to be rebooted
(scheduling of drained Pod's will only happen onto hosts that have already been
rebooted).

Usage example:
    cookbook sre.k8s.reboot-nodes --alias wikikube-staging-worker-codfw -g main --batchsize 1 reboot

This command will cause a rolling reboot of the nodes in the Kubernetes-staging
cluster, one at a time per taint-group, waiting 35 seconds before rebooting.
"""

from argparse import Namespace

from cookbooks.sre.k8s import K8sBatchBase, K8sBatchRunnerBase


class RollRebootK8sNodes(K8sBatchBase):
    """Kubernetes cluster nodes reboot"""

    batch_default = 1
    batch_max = 20
    valid_actions = ("reboot",)

    def get_runner(self, args: Namespace) -> "RollRebootK8sNodesRunner":
        """As specified by Spicerack API."""
        return RollRebootK8sNodesRunner(args, self.spicerack)


class RollRebootK8sNodesRunner(K8sBatchRunnerBase):
    """Group all nodes of a Kubernetes cluster by taints and perform rolling reboots on a per taint-group basis"""

    depool_threshold = 20  # Maximum allowed batch size
