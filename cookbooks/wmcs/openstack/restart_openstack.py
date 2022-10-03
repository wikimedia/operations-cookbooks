"""WMCS openstack - restart openstack services"""

import argparse
import logging

from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.common import (
    CommonOpts,
    SALLogger,
    WMCSCookbookRunnerBase,
    add_common_opts,
    run_one_raw,
    with_common_opts,
)
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.libs.openstack.common import OpenstackAPI

LOGGER = logging.getLogger(__name__)


class OpenstackRestart(CookbookBase):
    """WMCS Openstack cookbook to restart services."""

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(prog=__name__, description=__doc__, formatter_class=ArgparseFormatter)
        add_common_opts(parser)
        parser.add_argument(
            "--cluster-name",
            required=True,
            choices=list(OpenstackClusterName),
            type=OpenstackClusterName,
            help="Openstack cluster/deployment to act on.",
        )
        parser.add_argument("--all", action="store_true", help="Restart all openstack services")
        parser.add_argument("--nova", action="store_true", help="Restart all openstack nova services")
        parser.add_argument("--glance", action="store_true", help="Restart all openstack glance services")
        parser.add_argument("--keystone", action="store_true", help="Restart all openstack keystone services")
        parser.add_argument("--cinder", action="store_true", help="Restart all openstack cinder services")
        parser.add_argument(
            "--neutron", action="store_true", help="Restart all openstack neutron services except for neutron-l3-agent"
        )
        parser.add_argument("--trove", action="store_true", help="Restart all openstack trove services")
        parser.add_argument("--magnum", action="store_true", help="Restart all openstack magnum services")
        parser.add_argument("--heat", action="store_true", help="Restart all openstack magnum services")
        parser.add_argument("--swift", action="store_true", help="Restart all openstack swift services")
        parser.add_argument("--designate", action="store_true", help="Restart all openstack swift services")

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(spicerack=self.spicerack, args=args, runner=OpenstackRestartRunner)(
            spicerack=self.spicerack, cluster_name=args.cluster_name, args=args
        )


class OpenstackRestartRunner(WMCSCookbookRunnerBase):
    """Runner for QuotaIncrease"""

    def __init__(
        self,
        spicerack: Spicerack,
        cluster_name: OpenstackClusterName,
        args: argparse.Namespace,
        common_opts: CommonOpts,
    ):
        """Init"""
        self.common_opts = common_opts
        self.sallogger = SALLogger(project="admin", task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg)
        self.cluster_name = cluster_name
        self.args = args
        self.nova_services = None
        super().__init__(spicerack=spicerack)
        self.openstack_api = OpenstackAPI(remote=spicerack.remote(), cluster_name=cluster_name)

    # OpenStack services will give us info about hosts and services, but in a different format
    #  depending on the service. These little helper functions adjust that into standard
    #  (host, service) pairs
    def get_nova_service_list(self):
        """Get a list of registered nova services + hosts from OpenStack"""
        # Cache in case this gets called twice
        if not self.nova_services:
            service_info = self.openstack_api.get_nova_services()
            self.nova_services = [(service["Host"], service["Binary"]) for service in service_info]
        return self.nova_services

    def get_designate_service_list(self):
        """Get a list of registered designate services + hosts from OpenStack"""
        service_info = self.openstack_api.get_designate_services()
        return [(service["hostname"], "designate-%s" % service["service_name"]) for service in service_info]

    def get_neutron_service_list(self):
        """Get a list of registered neutron services + hosts from OpenStack"""
        service_info = self.openstack_api.get_neutron_services()
        # We never want to automatically restart the l3 agents, that can cause downtime.
        return [(service["Host"], service["Binary"]) for service in service_info if "l3-agent" not in service["Binary"]]

    def get_cinder_service_list(self):
        """Get a list of registered cinder services + hosts from OpenStack"""
        service_info = self.openstack_api.get_cinder_services()
        return [(service["Host"].removesuffix("@rbd"), service["Binary"]) for service in service_info]

    def get_misc_service_list(self, service):
        """Get a list of unregistered OpenStack services.

        There are several services that don't provide a useful discovery mechanism, all running on cloudcontrols.
         This function cheats and gets the cloudcontrols out of the nova service list,
         then hardcodes those services into the standard dict format.
        """
        cloudcontrol_service_list = {
            "glance": ["glance-api"],
            "keystone": ["keystone", "keystone-admin"],
            "trove": ["trove-api", "trove-conductor", "trove-taskmanager"],
            "heat": ["heat-api", "heat-api-cfn", "heat-engine"],
            "magnum": ["magnum-api", "magnum-conductor"],
        }

        cloudcontrols = {s[0] for s in self.get_nova_service_list() if s[0].startswith("cloudcontrol")}
        servicelist = []
        for servicename in cloudcontrol_service_list[service]:
            servicelist.extend([(cloudcontrol, servicename) for cloudcontrol in cloudcontrols])

        return servicelist

    def consolidate_restart_list(self, restart_list):
        """We want to make only one call per host. Fortunately, systemctl takes a list."""
        restart_dict = {}
        for pair in restart_list:
            if pair[0] not in restart_dict:
                restart_dict[pair[0]] = [pair[1]]
            else:
                restart_dict[pair[0]].append(pair[1])
        return restart_dict

    def restart_services(self, restart_dict: dict):
        """Restart services specified in a dict of hostname:[service]"""
        for host in restart_dict:
            # We still need to do a lookup because we didn't get fqdns from
            #  openstack.
            query = "P{%s*}" % host
            nodes = self.spicerack.remote().query(query, use_sudo=True)
            command = ["systemctl", "restart"] + restart_dict[host]
            print("Running %s on %s" % (command, nodes))
            run_one_raw(node=nodes, command=command)

    def run_with_proxy(self) -> None:
        """Main entry point"""
        restart_list = []
        if vars(self.args)["nova"] or self.args.all:
            restart_list.extend(self.get_nova_service_list())
        if vars(self.args)["cinder"] or self.args.all:
            restart_list.extend(self.get_cinder_service_list())
        if vars(self.args)["neutron"] or self.args.all:
            restart_list.extend(self.get_neutron_service_list())
        if vars(self.args)["designate"] or self.args.all:
            restart_list.extend(self.get_designate_service_list())
        if vars(self.args)["trove"] or self.args.all:
            restart_list.extend(self.get_misc_service_list("trove"))
        if vars(self.args)["keystone"] or self.args.all:
            restart_list.extend(self.get_misc_service_list("keystone"))
        if vars(self.args)["glance"] or self.args.all:
            restart_list.extend(self.get_misc_service_list("glance"))
        if vars(self.args)["magnum"] or self.args.all:
            restart_list.extend(self.get_misc_service_list("magnum"))
        if vars(self.args)["heat"] or self.args.all:
            restart_list.extend(self.get_misc_service_list("heat"))

        if restart_list:
            restart_dict = self.consolidate_restart_list(restart_list)
            self.restart_services(restart_dict)
        else:
            print("No restarts requested.")
