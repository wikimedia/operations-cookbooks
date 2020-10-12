"""Restart of the AQS nodejs service."""
import argparse
import logging

from datetime import timedelta

from spicerack.interactive import ask_confirmation, ensure_shell_is_durable

from cookbooks import ArgparseFormatter


__title__ = 'Roll restart all the nodejs service daemons on the AQS cluster'
logger = logging.getLogger(__name__)


def argument_parser():
    """As specified by Spicerack API."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=ArgparseFormatter)
    parser.add_argument(
        'cluster', help='The name of the AQS cluster to work on.', choices=['aqs'])
    return parser


def run(args, spicerack):
    """Restart all AQS nodejs service daemons on a given cluster"""
    ensure_shell_is_durable()
    remote = spicerack.remote()
    confctl = spicerack.confctl('node')
    aqs_canary = remote.query('A:' + args.cluster + '-canary')
    aqs_workers = remote.query('A:' + args.cluster)
    icinga = spicerack.icinga()
    reason = spicerack.admin_reason('Roll restart of all AQS\'s nodejs daemons.')

    ask_confirmation(
        'If a config change is being rolled-out, please run puppet on all hosts '
        'before proceeding.')

    with icinga.hosts_downtimed(aqs_workers.hosts, reason,
                                duration=timedelta(minutes=60)):
        logger.info("Depool and test on canary: %s", aqs_canary.hosts)
        aqs_canary.run_sync(
            'depool',
            'systemctl restart aqs'
        )
        ask_confirmation('Please test aqs on the canary.')
        logger.info('Pool the canary back.')
        aqs_canary.run_sync('pool')

        aqs_lbconfig = remote.query_confctl(
            confctl, cluster=args.cluster,
            name=r'(?!' + aqs_canary.hosts[0] + ').*')

        logger.info('Restarting remaining daemons (one host at the time).')
        aqs_lbconfig.run(
            'systemctl restart aqs', svc_to_depool=['aqs'],
            batch_size=1, max_failed_batches=2,
            batch_sleep=30.0)

    logger.info("All AQS service restarts completed!")
