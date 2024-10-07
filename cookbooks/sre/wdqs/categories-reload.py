"""Categories graph data reload."""

import argparse
import logging
from datetime import timedelta
from time import sleep

from spicerack import RemoteHosts, Reason, PuppetHosts, ConftoolEntity, AlertingHosts
from spicerack.cookbook import CookbookBase, CookbookRunnerBase, LockArgs

from cookbooks.sre.wdqs import StopWatch, is_behind_lvs

logger = logging.getLogger(__name__)


class CategoryReload(CookbookBase):
    """Category reload cookbook

    Usage example:
        # for lvs-managed hosts
        cookbook sre.wdqs.categories-reload --reason "bring new hosts into rotation" \
        --task-id T301167 wdqs1004.eqiad.wmnet

        # hosts not managed by lvs (note the --no-depool flag)
        cookbook sre.wdqs.categories-reload --no-depool \
        --reason "reloading on test host" --task-id T301167 wdqs1009.eqiad.wmnet
    """

    def argument_parser(self) -> argparse.ArgumentParser:
        """Parse the command line arguments for this cookbook."""
        parser = super().argument_parser()
        parser.add_argument('host', help='select a single WDQS host.')
        parser.add_argument('--task-id', help='task id for the change')
        parser.add_argument('--reason', required=True, help='Administrative Reason')
        parser.add_argument('--downtime', type=int, default=4, help='Hour(s) of downtime')
        parser.add_argument('--no-depool', action='store_true',
                            help='Don\'t depool host (use for non-lvs-managed hosts)')
        return parser

    def get_runner(self, args: argparse.Namespace) -> CookbookRunnerBase:
        """Get category reload cookbook runner."""
        remote = self.spicerack.remote()
        remote_host = remote.query(args.host)

        if len(remote_host) != 1:
            raise ValueError(f"Only one host is needed. Not {len(remote_host)}({remote_host})")

        return CategoryReloadRunner(
            remote_host=remote_host,
            puppet=self.spicerack.puppet(remote_host),
            confctl=self.spicerack.confctl('node'),
            alerting_host=self.spicerack.alerting_hosts(remote_host.hosts),
            reason=self.spicerack.admin_reason(args.reason, task_id=args.task_id),
            downtime=args.downtime,
            no_depool=args.no_depool
        )


class CategoryReloadRunner(CookbookRunnerBase):
    """Category reload runner"""

    def __init__(self,  # pylint: disable=too-many-arguments
                 *,
                 remote_host: RemoteHosts,
                 puppet: PuppetHosts,
                 confctl: ConftoolEntity,
                 alerting_host: AlertingHosts,
                 reason: Reason,
                 downtime: int,
                 no_depool: bool):
        """Create the runner."""
        self.remote_host = remote_host
        self.puppet = puppet
        self.confctl = confctl
        self.alerting_host = alerting_host
        self.reason = reason
        self.downtime = downtime
        self.no_depool = no_depool

    def _reload_categories(self):
        """Execute commands on host to reload categories data."""
        logger.info('Preparing to load data for categories')

        # TODO: consider keeping a copy of the journal and implement CookbookRunnerBase.rollback
        #  to recover from a failure.
        with self.puppet.disabled(self.reason):
            self.remote_host.run_sync(
                'systemctl stop wdqs-categories',
                'rm -fv /srv/wdqs/categories.jnl',
                'systemctl start wdqs-categories'
            )

        # Wait for blazegraph to be up
        # TODO: sleeping is far from ideal, consider using another technique (ping some blazegraph API?)
        #  to wait for its availability
        sleep(30)
        logger.info('Loading data for categories')
        watch = StopWatch()
        self.remote_host.run_sync(
            'test -f /srv/wdqs/categories.jnl',
            '/usr/local/bin/reloadCategories.sh wdqs'
        )
        logger.info('Categories loaded in %s', watch.elapsed())

    @property
    def lock_args(self) -> LockArgs:
        """Allow only one reload per host at a time."""
        return LockArgs(suffix=str(self.remote_host.hosts), concurrency=1,
                        ttl=int(timedelta(hours=self.downtime * 4).total_seconds()))

    @property
    def runtime_description(self) -> str:
        """Runtime description."""
        return f"reloading categories to {self.remote_host.hosts}"

    def run(self) -> None:
        """Required by Spicerack API."""
        with self.alerting_host.downtimed(self.reason, duration=timedelta(hours=self.downtime)):
            if self.no_depool or not is_behind_lvs(self.confctl, self.remote_host):
                self._reload_categories()
            else:
                with self.confctl.change_and_revert('pooled', True, False, name=str(self.remote_host)):
                    sleep(180)
                    self._reload_categories()
