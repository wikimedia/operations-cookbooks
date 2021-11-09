"""Restart of the AQS nodejs service."""
import argparse
import logging

from datetime import timedelta

from spicerack.cookbook import CookbookBase, CookbookRunnerBase
from wmflib.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter

logger = logging.getLogger(__name__)


class RollRestart(CookbookBase):
    """Roll restart all the nodejs service daemons on the AQS cluster.

    - Requires a single argument which sets the name of the AQS cluster to work on.

    Usage example:
        cookbook.sre.aqs.roll_restart aqs

    """

    def argument_parser(self):
        """As specified by Spicerack API."""
        parser = argparse.ArgumentParser(description=self.__doc__, formatter_class=ArgparseFormatter)
        parser.add_argument(
            'cluster', help='The name of the AQS cluster to work on.', choices=['aqs'])
        return parser

    def get_runner(self, args):
        """As specified by Spicerack API."""
        return RollRestartRunner(args, self.spicerack)


class RollRestartRunner(CookbookRunnerBase):
    """AQS Roll Restart cookbook runner class"""

    def __init__(self, args, spicerack):
        """Initialize the runner."""
        ensure_shell_is_durable()
        self.cluster = args.cluster
        self.remote = spicerack.remote()
        self.confctl = spicerack.confctl('node')
        self.aqs_canary = self.remote.query('A:' + args.cluster + '-canary')
        self.aqs_workers = self.remote.query('A:' + args.cluster)
        self.icinga_hosts = spicerack.icinga_hosts(self.aqs_workers.hosts)
        self.admin_reason = spicerack.admin_reason('Roll restart of all AQS\'s nodejs daemons.')

    @property
    def runtime_description(self):
        """Return a nicely formatted string that represents the cookbook action."""
        return 'for AQS {} cluster: {}'.format(self.cluster, self.admin_reason.reason)

    def run(self):
        """Required by Spicerack API."""
        ask_confirmation(
            'If a config change is being rolled-out, please run puppet on all hosts '
            'before proceeding.')

        with self.icinga_hosts.downtimed(self.admin_reason, duration=timedelta(minutes=60)):
            logger.info("Depool and test on canary: %s", self.aqs_canary.hosts)
            self.aqs_canary.run_sync(
                'depool',
                'systemctl restart aqs'
            )
            ask_confirmation('Please test aqs on the canary.')
            logger.info('Pool the canary back.')
            self.aqs_canary.run_sync('pool')

            aqs_lbconfig = self.remote.query_confctl(
                self.confctl, cluster=self.cluster,
                name=r'(?!' + self.aqs_canary.hosts[0] + ').*')

            logger.info('Restarting remaining daemons (one host at a time).')
            aqs_lbconfig.run(
                'systemctl restart aqs', svc_to_depool=['aqs'],
                batch_size=1, max_failed_batches=2,
                batch_sleep=30.0)

        logger.info("All AQS service restarts completed!")
