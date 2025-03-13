"""Restart/Upgrade Liberica"""

import argparse
from contextlib import nullcontext
from datetime import timedelta

from spicerack import Reason
from spicerack.decorators import retry
from spicerack.remote import RemoteExecutionError, RemoteHosts
from wmflib.constants import ALL_DATACENTERS
from wmflib.interactive import confirm_on_failure

from cookbooks.sre import SREBatchBase, SREBatchRunnerBase


class RollUpgradeLiberica(SREBatchBase):
    r"""Roll upgrade/restart Liberica daemons

    * [Optional] Depool
    * [Only for upgrade action] Install specified liberica version
    * Restart liberica related daemons
    * [Optional] Repool

    Example usage:

        # seamless upgrade (no depool required)
        cookbook sre.loadbalancer.upgrade \
                --seamless \
                --alias liberica-canary \
                --reason '0.11 upgrade' \
                --version '0.11' upgrade

        # depool before upgrading
        cookbook sre.loadbalancer.upgrade \
                --alias liberica-canary \
                --reason '0.11 upgrade' \
                --version '0.11' upgrade

        # restart userspace daemons without depooling
        # this could be useful to change some config settings
        # that aren't reloadable like gRPC or prometheus bind address
        cookbook sre.loadbalancer.upgrade \
                --seamless \
                --alias liberica-canary \
                --reason 'config reload' \
                restart

    """

    batch_default = 1
    batch_max = 1
    valid_actions = ('upgrade', 'restart')

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = super().argument_parser()
        parser.add_argument('--version', type=str,
                            help='version of Liberica to upgrade to')
        parser.add_argument('--seamless', action='store_true',
                            help='restart/upgrade daemons without depooling the instance(s)')

        return parser

    def get_runner(self, args: argparse.Namespace):
        """As required by Spicerack API"""
        if args.action == "upgrade" and args.version is None:
            raise RuntimeError('--version is mandatory for upgrade action')

        return RollUpgradeLibericaRunner(args, self.spicerack)


class RollUpgradeLibericaRunner(SREBatchRunnerBase):
    """Controller class for liberica upgrade/restart operations"""

    @property
    def allowed_aliases(self) -> list:
        """List of allowed aliases for host selection"""
        base_aliases = ["liberica", "liberica-canary"]
        all_aliases = []
        for base in base_aliases:
            all_aliases.append(base)
            for datacenter in ALL_DATACENTERS:
                all_aliases.append(f"{base}-{datacenter}")

        return all_aliases

    @property
    def allowed_aliases_query(self) -> str:
        """Helper property to return a cumin formatted query of allowed aliases"""
        return "A:liberica"

    @property
    def runtime_description(self) -> str:
        """pretty-print message"""
        msg = f"{self._args.action}ing {self._query()}"
        if self._args.task_id:
            msg += f" ({self._args.task_id})"
        return msg

    def _upgrade_action(self, hosts: RemoteHosts, reason: Reason) -> int:
        """Upgrade liberica and restart the daemons"""
        if not self._args.seamless:
            self._admin_cookbook("depool")

        self._run(hosts, reason, upgrade=True)

        if not self._args.seamless:
            self._admin_cookbook("pool")

        return 0

    def _restart_action(self, hosts: RemoteHosts, reason: Reason) -> int:
        """Restart liberica daemons"""
        if not self._args.seamless:
            self._admin_cookbook("depool")

        self._run(hosts, reason, upgrade=False)

        if not self._args.seamless:
            self._admin_cookbook("pool")

        return 0

    def _upgrade(self, hosts: RemoteHosts) -> None:
        apt_get = self._spicerack.apt_get(hosts)
        confirm_on_failure(apt_get.update)
        confirm_on_failure(apt_get.install, f"liberica={self._args.version}")

    def _run(self, hosts: RemoteHosts, reason: Reason, upgrade: bool) -> int:
        stop_svcs = ['fp', 'healthcheck', 'hcforwarder']
        if self._args.seamless:
            stop_svcs.insert(0, 'cp')  # on seamless mode the depool is skipped so liberica-cp is still running
            signal = 'SIGTERM'
            disable_puppet = True
        else:
            signal = 'SIGUSR1'
            disable_puppet = False  # puppet has been already disabled by the admin cookbook

        start_svcs = list(reversed(stop_svcs))

        stop_cmds = [
            f"/bin/systemctl kill liberica-{svc}.service --signal {signal}"
            for svc in stop_svcs
        ]

        start_cmds = [
            f"/bin/systemctl start liberica-{svc}.service"
            for svc in start_svcs
        ]

        puppet = self._spicerack.puppet(hosts)

        with puppet.disabled(reason) if disable_puppet else nullcontext():
            # stop the services
            confirm_on_failure(hosts.run_sync, *stop_cmds)
            # validate that liberica services have been stopped
            self._check_liberica_is_stopped(hosts)
            # upgrade liberica if needed
            if upgrade:
                self._upgrade(hosts)
            # re-start the services
            confirm_on_failure(hosts.run_sync, *start_cmds)

        return 0

    @retry(tries=5, delay=timedelta(seconds=3), backoff_mode='linear', exceptions=(RemoteExecutionError,))
    def _check_liberica_is_stopped(self, hosts: RemoteHosts) -> None:
        """Check if liberica services have been stopped"""
        cmd = ' | '.join([
            '/bin/systemctl show --property MainPID --value liberica-*.service',
            'grep -v "^$"',
            'paste -s -d","',
            'grep "^[0,]+$"',  # systemctl returns MainPID=0 if the service isn't running
        ])
        hosts.run_sync(cmd)

    def _admin_cookbook(self, action: str) -> None:
        """Trigger sre.loadbalancer.admin cookbook"""
        args = [
            '--reason', self._args.reason,
            action
        ]

        if self._args.query:
            args.extend(('--query', self._args.query))
        else:
            args.extend(('--alias', self._args.alias))

        self._spicerack.run_cookbook('sre.loadbalancer.admin', args, confirm=True)
