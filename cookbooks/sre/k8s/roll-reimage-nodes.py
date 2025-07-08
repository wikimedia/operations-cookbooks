"""Rolling-reimage a set of nodes in a kubernetes cluster.

This cookbook will:
1. Cordon all hosts (to avoid rescheduling churn);
2. For each host, in batches:
   - Drain the host
   - Set its pooled status to inactive
   - Set Icinga/Alertmanager downtime for the host
   - Reimage the host
   - Wait for the host to come back online
   - Wait for Icinga optimal status
   - Remove the Icinga/Alertmanager downtime
   - Set the host's pooled status to yes
   - Uncordon the host

The cookbook will refuse to reimage all nodes in a taint group.

Usage example:
    cookbook sre.k8s.roll-reimage-nodes --k8s-cluster staging-codfw \
        --query 'P{kubestage200[1-3,6-7].codfw.wmnet}' --reason treason

This command will cause a rolling reimage of the selected nodes in the Kubernetes-staging
cluster, waiting 35 seconds between reimages.
"""

from argparse import ArgumentParser, Namespace

from spicerack import Spicerack
from spicerack.administrative import Reason
from spicerack.remote import RemoteHosts

from wmflib.interactive import ask_confirmation

from cookbooks.sre.hosts import OS_VERSIONS
from cookbooks.sre.k8s import K8sBatchBase, K8sBatchRunnerBase


class RollReimageK8sNodes(K8sBatchBase):
    """Kubernetes cluster nodes rolling-reimage"""

    batch_default = 3
    batch_max = 6

    valid_actions = ("reimage",)

    def argument_parser(self) -> ArgumentParser:
        """Parse arguments"""
        parser = super().argument_parser()

        parser.add_argument(
            '--os', choices=OS_VERSIONS, required=True,
            help='the Debian version to install.')

        # TODO --new is complicated because _hosts() won't work when the nodes aren't in puppet.
        # I will add support for it after the basic version is merged.
        # Note: --move-vlan is unsupported ATM, as it needs out-of-cookbook homer changes that
        # wouldn't work well in this sort of non-interactive workflow.

        return parser

    def get_runner(self, args: Namespace) -> "RollReimageK8sNodesRunner":
        """As specified by Spicerack API."""
        # TODO check the taint thing here maybe?
        return RollReimageK8sNodesRunner(args, self.spicerack)


class RollReimageK8sNodesRunner(K8sBatchRunnerBase):
    """Perform rolling reimages on a set of nodes."""

    depool_threshold = 6  # Maximum allowed batch size

    def __init__(self, args: Namespace, spicerack: Spicerack) -> None:
        """Initialize the runner."""
        super().__init__(args, spicerack)

        # We do not ask for confirmation for individual hosts, so ask once here.
        ask_confirmation(f'Will reimage {len(self.all_hosts)} nodes: {self.all_hosts}. Proceed?')

        # ask for mgmt password early, it will be stored
        _ = self._spicerack.management_password()

    def run(self) -> int:
        """Perform rolling reimages on a set of nodes."""
        res = super().run()

        # Post to phab when all reimages have been completed
        self.phabricator.task_comment(
            self._args.task_id,
            (
                f"Cookbook {__name__} -- {self.runtime_description} completed:"
                f"\n{self._spicerack.actions}\n"
            ),
        )

        return res

    def _reimage_action(self, hosts: RemoteHosts, _: Reason) -> None:
        """Reimage a set of hosts.

        Called by the parent class for each batch.

        At this point, we are:
        - cordoned and drained (by K8sBatchRunnerBase.pre_action)
        - downtimed (by SREBatchRunnerBase.action, which called this)
        - depooled (by SRELBBatchRunnerBase.action, which called SREBatchBaseRunner.action)
        """
        for node in hosts.hosts:
            hostname = node.split('.')[0]  # reimage takes just the hostname, not the FQDN
            # skip initial confirmation, as we ask once at the beginning;
            # pass --new, as the reimage cookbook unsets it when not needed
            reimage_args = ['--force', '--new', '--puppet', '7', '--os', self._args.os, hostname]

            self._spicerack.run_cookbook('sre.hosts.reimage', reimage_args, confirm=True)

            # The reimage cookbook puts spicerack actions under just the hostname, while this (and
            # the parent class) uses the FQDN, so we patch it here.
            # Currently, overwriting it at this point doesn't lose any information,
            # as this cookbook doesn't write into it. Future me will surely appreciate this.
            self._spicerack.actions[node] = self._spicerack.actions.pop(hostname)

        # Propagate errors upwards so we don't repool broken nodes.
        # This will leave the whole batch depooled unnecessarily, but it is simple.
        for node in hosts.hosts:
            if self._spicerack.actions[node].has_failures:
                raise RuntimeError(f"Reimaging node {node} failed, bailing out.")
