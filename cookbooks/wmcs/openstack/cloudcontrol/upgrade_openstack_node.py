r"""WMCS openstack - upgrade a cloudcontrol or cloudservices node and reboot

Usage example: wmcs.openstack.cloudvirt.upgrade_openstack_node \
    --fqdn-to-upgrade cloudvirt1013.eqiad.wmnet

"""
import argparse
import logging
from datetime import datetime

from cumin.transports import Command
from spicerack import Spicerack
from spicerack.cookbook import ArgparseFormatter, CookbookBase, CookbookRunnerBase

from cookbooks.wmcs.libs.alerts import downtime_host, uptime_host
from cookbooks.wmcs.libs.common import CommonOpts, SALLogger, add_common_opts, run_one_raw, with_common_opts
from cookbooks.wmcs.libs.inventory import OpenstackClusterName
from cookbooks.wmcs.openstack.network.tests import NetworkTests

LOGGER = logging.getLogger(__name__)


def check_network_ok(cluster_name: OpenstackClusterName, spicerack: Spicerack) -> None:
    """Run the network tests and check if they pass."""
    args = ["--cluster_name", str(cluster_name)]
    network_test_cookbook = NetworkTests(spicerack=spicerack)
    if network_test_cookbook.get_runner(args=network_test_cookbook.argument_parser().parse_args(args)).run() != 0:
        raise Exception("Network tests failed, see logs or run the cookbook for details.")


class LiveUpgrade(CookbookBase):
    """WMCS Openstack cookbook to upgrade openstack

    Works on a cloudcontrol, cloudservices, or cloudbackup node. The host
    will be rebooted post-upgrade.

    The current version of openstack is presumed to have already been set
    via puppet along with provision of needed classes and files; if the
    specified node is already running the puppetized version of OpenStack
    this cookbook should do very little beyond rebooting.
    """

    __title__ = __doc__

    def argument_parser(self):
        """Parse the command line arguments for this cookbook."""
        parser = argparse.ArgumentParser(
            prog=__name__,
            description=__doc__,
            formatter_class=ArgparseFormatter,
        )
        add_common_opts(parser)
        parser.add_argument(
            "--fqdn-to-upgrade",
            required=True,
            help="FQDN of the cloudcontrol to upgrade.",
        )
        parser.add_argument(
            "--upgrade-dbs",
            required=False,
            action="store_false",
            help="If passed, upgrade openstack service databases. "
            "Only needs to happen once but should be harmless if repeated.",
        )

        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get runner"""
        return with_common_opts(
            self.spicerack,
            args,
            UpgradeRunner,
        )(fqdn_to_upgrade=args.fqdn_to_upgrade, spicerack=self.spicerack, upgrade_dbs=args.upgrade_dbs)


class UpgradeRunner(CookbookRunnerBase):
    """Runner for LiveUpgrade."""

    def __init__(
        self,
        common_opts: CommonOpts,
        fqdn_to_upgrade: str,
        spicerack: Spicerack,
        upgrade_dbs: bool,
    ):
        """Init."""
        self.fqdn_to_upgrade = fqdn_to_upgrade
        self.spicerack = spicerack
        self.upgrade_dbs = upgrade_dbs
        self.sallogger = SALLogger(
            project=common_opts.project, task_id=common_opts.task_id, dry_run=common_opts.no_dologmsg
        )
        self.common_opts = common_opts

    def run(self) -> None:
        """Main entry point."""
        node_to_upgrade = self.spicerack.remote().query(f"D{{{self.fqdn_to_upgrade}}}", use_sudo=True)

        host_name = self.fqdn_to_upgrade.split(".", 1)[0]
        puppet = self.spicerack.puppet(node_to_upgrade)
        host_silence_id = downtime_host(
            spicerack=self.spicerack,
            host_name=host_name,
            comment="Rebooting with wmcs.openstack.cloudcontrol.reboot_node",
            task_id=self.common_opts.task_id,
        )
        LOGGER.info("Silenced node %s with ID %s", self.fqdn_to_upgrade, host_silence_id)

        run_one_raw(node=node_to_upgrade, command=["puppet", "agent", "--enable"])
        puppet.run()

        puppet_reason = self.spicerack.admin_reason("Package and OpenStack upgrade")
        with puppet.disabled(puppet_reason):
            run_one_raw(node=node_to_upgrade, command=["apt", "update"])
            run_one_raw(
                node=node_to_upgrade,
                command=[
                    "DEBIAN_FRONTEND=noninteractive",
                    "apt-get",
                    "dist-upgrade",
                    "-y",
                    "--allow-downgrades",
                    "-o",
                    '"Dpkg::Options::=--force-confdef"',
                    "-o",
                    '"Dpkg::Options::=--force-confold"',
                ],
            )

        puppet.run()

        if self.upgrade_dbs:
            if "control" in self.fqdn_to_upgrade:
                # Back things up before upgrading.

                backuppath = "/root/openstack-db-backups/%s" % datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                run_one_raw(node=node_to_upgrade, command=Command("mkdir -p %s" % backuppath))

                dblist = ["cinder", "designate", "glance", "keystone", "neutron", "placement"]
                if "100" in self.fqdn_to_upgrade:
                    dblist.extend(
                        [
                            "eqiad1_heat",
                            "eqiad1_magnum",
                            "nova_api_eqiad1",
                            "nova_cell0_eqiad1",
                            "nova_eqiad1",
                            "trove_eqiad1",
                        ]
                    )
                elif "-dev" in self.fqdn_to_upgrade:
                    dblist.extend(
                        [
                            "barbican",
                            "codfw1dev_heat",
                            "codfw1dev_magnum",
                            "nova_api",
                            "nova_cell0",
                            "nova",
                            "trove_codfw1dev",
                        ]
                    )
                else:
                    LOGGER.info(
                        "Unable to determine deployment for node %s, skipping some database backups.",
                        self.fqdn_to_upgrade,
                    )
                for db in dblist:
                    # wrap this in anther shell because mysqldump requires file redirection
                    run_one_raw(
                        node=node_to_upgrade,
                        command=Command('sh -c "/usr/bin/mysqldump -u root %s > %s/%s.sql"' % (db, backuppath, db)),
                    )
                LOGGER.info("Backed up OpenStack databases to %s", backuppath)

                run_one_raw(node=node_to_upgrade, command=Command("nova-manage api_db sync"))
                run_one_raw(node=node_to_upgrade, command=Command("nova-manage db sync"))
                run_one_raw(node=node_to_upgrade, command=Command("placement-manage db sync"))
                run_one_raw(node=node_to_upgrade, command=Command("glance-manage db_sync"))
                run_one_raw(node=node_to_upgrade, command=Command("keystone-manage db_sync"))
                run_one_raw(node=node_to_upgrade, command=Command("cinder-manage db online_data_migrations"))
                run_one_raw(node=node_to_upgrade, command=Command("cinder-manage db sync"))
                run_one_raw(node=node_to_upgrade, command=Command("heat-manage db_sync"))
                run_one_raw(node=node_to_upgrade, command=Command("magnum-db-manage upgrade heads"))
                run_one_raw(node=node_to_upgrade, command=Command("trove-manage db_sync"))

        elif "services" in self.fqdn_to_upgrade:
            run_one_raw(node=node_to_upgrade, command=Command("designate-manage database sync"))

        puppet.run()

        if self.upgrade_dbs and "control" in self.fqdn_to_upgrade:
            run_one_raw(node=node_to_upgrade, command=Command("nova-manage db online_data_migrations"))
            run_one_raw(node=node_to_upgrade, command=Command("neutron-db-manage upgrade heads"))

        reboot_time = datetime.utcnow()
        node_to_upgrade.reboot()

        node_to_upgrade.wait_reboot_since(since=reboot_time)
        LOGGER.info(
            "Rebooted node %s, waiting for cluster to stabilize...",
            self.fqdn_to_upgrade,
        )

        uptime_host(spicerack=self.spicerack, host_name=host_name, silence_id=host_silence_id)
        LOGGER.info("Silences removed.")

        self.sallogger.log(f"Upgraded and rebooted host {self.fqdn_to_upgrade}")
