"""Ceph Clusters Operations"""

__owner_team__ = "Data Platform"

CEPHOSD_CODFW = "cephosd-codfw"
CEPHOSD_EQIAD = "cephosd-eqiad"
CLUSTER_CHOICES = (CEPHOSD_CODFW, CEPHOSD_EQIAD)
CLUSTER_ADMIN_HOST = {CEPHOSD_EQIAD: "ceph-admin1001.eqiad.wmnet", CEPHOSD_CODFW: "ceph-admin2001.codfw.wmnet"}
