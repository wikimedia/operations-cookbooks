"""Cookbook to perform a rolling reboot of LDAP replicas

Usage example:
    cookbook sre.ldap.roll-restart-reboot-replica --alias ldap-replica-codfw \
       --reason "Rolling reboot to pick up new kernel" reboot

"""

from cookbooks.sre import SREBatchBase, SRELBBatchRunnerBase


class LDAPReplicaRestartReboot(SREBatchBase):
    """Class to roll-restart or -reboot a cluster of LDAP replicas"""

    batch_default = 1

    # Required
    def get_runner(self, args):
        """As specified by Spicerack API."""
        return LDAPReplicaRestartRebootRunner(args, self.spicerack)


class LDAPReplicaRestartRebootRunner(SRELBBatchRunnerBase):
    """Roll reboot/restart an LDAP replica cluster"""

    @property
    def allowed_aliases(self):
        """Required by SRELatchRunnerBase"""
        return ['ldap-replicas', 'ldap-replicas-eqiad', 'ldap-replicas-codfw']

    @property
    def restart_daemons(self):
        """Return a list of daemons to restart when using the restart action"""
        return ['slapd']
